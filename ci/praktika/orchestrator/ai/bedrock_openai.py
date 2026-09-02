"""OpenAI-model-on-Bedrock provider (one-shot ``complete`` only).

Serves an OpenAI model (e.g. ``openai.gpt-oss-120b-1:0``) hosted on Amazon
Bedrock through the **Converse** API via ``boto3`` — no ``openai`` SDK and no
``ANTHROPIC_API_KEY``; auth is the standard AWS credential chain (env / shared
profile / instance role). ``boto3`` is a core Praktika dependency, so this
provider needs no extra install.

It implements the generic ``complete(system, user_content, tools, tool_executor)``
seam (the entry point ``praktika review`` uses) and runs its own tool-use loop
against Converse's ``toolUse`` / ``toolResult`` content blocks. It does **not**
implement the orchestrator lifecycle hooks — ``on_job_failure`` etc. stay the
inherited no-ops — so it is a review/standalone provider, not an advisor.

Region resolution mirrors the Anthropic ``BedrockProvider``: explicit
``aws_region`` arg → ``Settings.AWS_REGION`` → ``AWS_REGION`` /
``AWS_DEFAULT_REGION`` env. Bedrock Runtime has no region fallback, so a region
must be resolvable or ``complete`` raises (→ the caller's error handling).
"""
import json
import os
import time

from .provider import AIProvider, Turn, Usage

# Name of the synthetic tool used to collect structured output. When
# ``response_schema`` is set the model is required to return its answer by
# calling this tool, so the result is a validated argument object rather than
# free text a reasoning model tends to muddle with its analysis.
_SUBMIT_TOOL = "submit_result"

# Per-1M-token (input, output) USD pricing, matched by longest substring in the
# model id (tolerant of version suffixes). Unknown ids price at zero so cost
# accounting degrades gracefully rather than guessing.
_PRICING = {
    "gpt-oss-120b": (0.15, 0.60),
    "gpt-oss-20b": (0.07, 0.30),
}

# Stop runaway tool loops: at most this many tool rounds before a final answer
# is forced from whatever the model has seen. A large PR (many files) needs room
# to investigate before it is pushed to write up, so this is more generous than
# the Anthropic provider's cap.
_MAX_TOOL_ROUNDS = 12

# Appended to the system prompt for the forced final write-up once the model
# exhausts its tool-call budget: stop investigating and produce the review now,
# so the loop yields real text instead of another tool call.
_STOP_AND_WRITE_UP = (
    "\n\nYou have reached the tool-call budget and investigated enough. Do NOT "
    "call any more tools. Write your complete code review now as your final "
    "text answer, covering every issue you found."
)


def _budget_note(round_number, total_rounds):
    """System-prompt suffix telling the model where it is in its tool budget, so
    it can plan to write up before it is force-stopped. The final round returns
    the hard stop directive."""
    remaining = total_rounds - round_number
    if remaining <= 0:
        return _STOP_AND_WRITE_UP
    return (
        f"\n\nTool-call budget: round {round_number} of {total_rounds} "
        f"({remaining} left before you must stop). Investigate what you need, "
        "then write your complete review as a text answer - do not spend the "
        "whole budget on tool calls."
    )


def _price_per_mtok(model):
    for key in sorted(_PRICING, key=len, reverse=True):
        if key in (model or ""):
            return _PRICING[key]
    return (0.0, 0.0)


def _to_tool_config(tools):
    """Translate neutral tool dicts (name/description/input_schema — the shape
    the Anthropic provider and the review job already build) into a Converse
    ``toolConfig``. Returns None when there are no tools."""
    if not tools:
        return None
    specs = []
    for t in tools:
        specs.append(
            {
                "toolSpec": {
                    "name": t["name"],
                    "description": t.get("description", ""),
                    "inputSchema": {"json": t.get("input_schema", {"type": "object"})},
                }
            }
        )
    return {"tools": specs}


class BedrockOpenAIProvider(AIProvider):
    name = "bedrock-openai"
    # gpt-5.6 via its global inference profile (on-demand invocation of the bare
    # model id isn't supported), close to the ClickHouse code-review job's
    # gpt-5.x. Override with --model, e.g. global.openai.gpt-5.6-terra / -luna,
    # or openai.gpt-oss-120b-1:0.
    DEFAULT_MODEL = "global.openai.gpt-5.6-sol"

    # Default reasoning effort for investigation turns. "low" is used for the
    # forced final submit / summary so the model spends its output budget
    # emitting the result, not more analysis.
    DEFAULT_REASONING_EFFORT = "high"

    def __init__(self, model="", aws_region="", reasoning_effort=""):
        super().__init__(model=model)
        self.aws_region = aws_region or ""
        self.reasoning_effort = reasoning_effort or self.DEFAULT_REASONING_EFFORT
        self._client = None  # lazily constructed on first complete()

    def _reasoning_fields(self, effort):
        """The ``additionalModelRequestFields`` for a given reasoning effort,
        in the shape the target model family expects. gpt-oss takes a flat
        ``reasoning_effort`` (max "high"); gpt-5.x takes a nested
        ``reasoning: {effort}`` (supports "xhigh")."""
        model = self.resolved_model()
        if "gpt-oss" in model:
            # gpt-oss has no "xhigh"; clamp so a shared default still works.
            eff = "high" if effort == "xhigh" else effort
            return {"reasoning_effort": eff}
        return {"reasoning": {"effort": effort}}

    def _region(self):
        if self.aws_region:
            return self.aws_region
        from praktika.settings import Settings

        return (
            getattr(Settings, "AWS_REGION", "")
            or os.environ.get("AWS_REGION")
            or os.environ.get("AWS_DEFAULT_REGION")
            or ""
        )

    def _get_client(self):
        if self._client is None:
            try:
                import boto3
            except ImportError as e:  # boto3 is a core dep; guard anyway
                raise RuntimeError(
                    "boto3 not installed; required for AI_PROVIDER='bedrock-openai'"
                ) from e
            region = self._region()
            if not region:
                raise RuntimeError(
                    "no AWS region for Bedrock; set Settings.AWS_REGION or AWS_REGION"
                )
            self._client = boto3.client("bedrock-runtime", region_name=region)
        return self._client

    def complete(
        self,
        system,
        user_content,
        tools=None,
        tool_executor=None,
        max_tokens=4000,
        response_schema=None,
    ) -> Turn:
        client = self._get_client()
        model = self.resolved_model()
        inv_config = _to_tool_config(list(tools or []))

        messages = [{"role": "user", "content": [{"text": user_content}]}]
        totals = {"input": 0, "output": 0}
        tool_calls = 0
        total_rounds = _MAX_TOOL_ROUNDS + 1

        def _record(resp):
            u = resp.get("usage") or {}
            totals["input"] += u.get("inputTokens", 0) or 0
            totals["output"] += u.get("outputTokens", 0) or 0
            return ((resp.get("output") or {}).get("message") or {}).get("content") or []

        t0 = time.time()

        # ---- Phase 1: investigate and produce free-text findings ----------
        # gpt-oss is a reasoning model that writes its answer as prose; let it.
        # We don't ask for JSON here — structuring happens in phase 2, which is
        # far more reliable than parsing JSON out of a reasoning model's output.
        text = ""
        interim = []
        tool_rounds = 0
        exhausted = True
        for round_index in range(total_rounds):
            kwargs = dict(
                modelId=model,
                system=[{"text": system + _budget_note(round_index + 1, total_rounds)}],
                messages=messages,
                inferenceConfig={"maxTokens": max_tokens},
                additionalModelRequestFields=self._reasoning_fields(
                    self.reasoning_effort
                ),
            )
            if inv_config:
                kwargs["toolConfig"] = inv_config
            content = _record(client.converse(**kwargs))
            round_text = next((b["text"] for b in content if "text" in b), "")
            tool_uses = [b["toolUse"] for b in content if "toolUse" in b]
            if not tool_uses:
                # A terminating no-tool turn is the authoritative write-up; it
                # supersedes any interim text kept for salvage below.
                text = round_text or text
                exhausted = False
                break
            tool_rounds += 1
            # A reasoning model often emits a text block alongside its tool
            # calls; accumulate those so an interim write-up survives even when
            # the loop later hits the round cap without a clean final turn.
            if round_text:
                interim.append(round_text)
            # Echo the assistant turn back, but strip reasoningContent blocks:
            # Bedrock rejects prior-turn reasoningContent in the request for
            # gpt-oss (ValidationException). Only toolUse/text are needed to
            # continue the tool loop.
            echo = [b for b in content if "reasoningContent" not in b]
            messages.append({"role": "assistant", "content": echo})
            results = []
            for tu in tool_uses:
                tool_calls += 1
                out = (
                    tool_executor(tu["name"], tu.get("input") or {})
                    if tool_executor is not None
                    else f"error: no tool executor for {tu['name']!r}"
                )
                results.append(
                    {
                        "toolResult": {
                            "toolUseId": tu["toolUseId"],
                            "content": [{"text": out}],
                        }
                    }
                )
            messages.append({"role": "user", "content": results})

        if exhausted:
            # The model kept calling tools to the round cap; ask it once more to
            # write up its findings (messages ends with a user turn). toolConfig
            # must still be passed — Converse rejects a request that omits it
            # while the history contains toolUse/toolResult blocks — but a firm
            # stop directive plus low reasoning effort biases it to answer in
            # text now instead of spending the turn on yet another tool call.
            kwargs = dict(
                modelId=model,
                system=[{"text": system + _STOP_AND_WRITE_UP}],
                messages=messages,
                inferenceConfig={"maxTokens": max_tokens},
                additionalModelRequestFields=self._reasoning_fields("low"),
            )
            if inv_config:
                kwargs["toolConfig"] = inv_config
            content = _record(client.converse(**kwargs))
            text = next((b["text"] for b in content if "text" in b), text)

        # No authoritative final/fallback text: fall back to the interim text
        # the model emitted alongside its tool calls so an early finding is not
        # lost to later progress chatter.
        if not (text or "").strip():
            text = "\n\n".join(interim).strip()

        # A blank write-up means the investigation never produced a review - it
        # spent its whole budget on tool calls. Structuring that would emit a
        # vacuous "no issues" result, so fail instead and let the caller retry.
        if not (text or "").strip():
            usage = self._usage(
                model, totals, int((time.time() - t0) * 1000),
                tool_calls=tool_calls, tool_rounds=tool_rounds,
                max_tool_rounds=total_rounds, exhausted=exhausted,
            )
            print(
                f"[AI {self.name}] complete: model={model} tool_calls={tool_calls} "
                f"tool_rounds={tool_rounds}/{total_rounds} "
                "structured=aborted (no review text produced)"
            )
            return Turn(
                reasoning="",
                usage=usage,
                error="model produced no review text after investigation",
            )

        # ---- Phase 2: structure the findings via a forced tool call -------
        # A separate, small-context call that forces ``submit_result``. gpt-oss
        # honors a forced ``toolChoice`` reliably when the context is small, so
        # the result comes back as validated tool arguments, not free text.
        if response_schema is not None:
            submit_spec = _to_tool_config(
                [
                    {
                        "name": _SUBMIT_TOOL,
                        "description": "Return the structured result.",
                        "input_schema": response_schema,
                    }
                ]
            )
            structure_msg = (
                "Convert the following code review into the required structured "
                "result by calling the " + _SUBMIT_TOOL + " tool. Preserve every "
                "finding, its file path and line, and the summary verbatim.\n\n"
                + (text or "(the reviewer produced no findings)")
            )
            forced_content = _record(
                client.converse(
                    modelId=model,
                    system=[{"text": "You convert a review into structured data."}],
                    messages=[{"role": "user", "content": [{"text": structure_msg}]}],
                    inferenceConfig={"maxTokens": max(max_tokens, 4000)},
                    additionalModelRequestFields=self._reasoning_fields("low"),
                    toolConfig={
                        "tools": submit_spec["tools"],
                        "toolChoice": {"tool": {"name": _SUBMIT_TOOL}},
                    },
                )
            )
            forced_submit = [
                b["toolUse"]
                for b in forced_content
                if "toolUse" in b and b["toolUse"]["name"] == _SUBMIT_TOOL
            ]
            if forced_submit:
                text = json.dumps(forced_submit[0].get("input") or {})

        latency_ms = int((time.time() - t0) * 1000)
        usage = self._usage(
            model, totals, latency_ms,
            tool_calls=tool_calls, tool_rounds=tool_rounds,
            max_tool_rounds=total_rounds, exhausted=exhausted,
        )
        print(
            f"[AI {self.name}] complete: model={model} tool_calls={tool_calls} "
            f"tool_rounds={tool_rounds}/{total_rounds}"
            f"{' exhausted' if exhausted else ''} "
            f"structured={response_schema is not None} "
            f"tokens={usage.input_tokens}/{usage.output_tokens}"
        )
        return Turn(reasoning=text, usage=usage)

    def _usage(self, model, totals, latency_ms, tool_calls=0, tool_rounds=0,
               max_tool_rounds=0, exhausted=False) -> Usage:
        inp = totals["input"]
        out = totals["output"]
        in_price, out_price = _price_per_mtok(model)
        cost = (inp * in_price + out * out_price) / 1_000_000
        return Usage(
            input_tokens=inp,
            output_tokens=out,
            cost_usd=round(cost, 6),
            latency_ms=latency_ms,
            provider=self.name,
            model=model,
            tool_calls=tool_calls,
            tool_rounds=tool_rounds,
            max_tool_rounds=max_tool_rounds,
            exhausted=exhausted,
        )
