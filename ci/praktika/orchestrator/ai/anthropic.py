"""Anthropic-backed AI provider (basic, advisory-only).

The first *real* ``AIProvider``: it sends the serialized ``Observation`` to the
Claude Messages API and turns the structured reply into a ``Turn`` (reasoning +
a non-actionable ``decision`` list) with token usage and a computed
``cost_usd``. Nothing it returns is applied to the run yet — same advisory-only
contract as the mock.

It implements the ``on_job_failure`` hook only — the model is consulted when a
job fails and never on a green job. The other lifecycle hooks stay the inherited
no-ops.

The ``anthropic`` SDK is an *optional* dependency: it is imported lazily inside
``on_job_failure`` so merely registering this provider never forces the import.
If the SDK or ``ANTHROPIC_API_KEY`` is missing, the hook raises — and
``AIProvider.consult`` converts that into an error ``Turn``, so a missing
dependency degrades the advisor to a no-op instead of crashing the orchestrator.
"""
import json
import os
import re
import time

from .provider import AIProvider, Turn, Usage

# Per-1M-token (input, output) USD pricing. Used to fill Usage.cost_usd — the
# seam the UsageLedger and per-PR budget caps already consume.
_PRICING = {
    "claude-opus-4-8": (5.0, 25.0),
    "claude-opus-4-7": (5.0, 25.0),
    "claude-sonnet-5": (3.0, 15.0),
    "claude-haiku-4-5": (1.0, 5.0),
}


def _price_per_mtok(model):
    """(input, output) $/1M for a model id, tolerant of backend prefixes.

    Bedrock runtime targets may be raw provider ids (``anthropic.*``) or
    inference-profile ids (for example ``eu.anthropic.claude-opus-4-8`` or
    ``global.anthropic.claude-sonnet-5``); the first-party id is bare. Match on
    the longest pricing key contained in the id.
    """
    for key in sorted(_PRICING, key=len, reverse=True):
        if key in model:
            return _PRICING[key]
    return (0.0, 0.0)

_SYSTEM = (
    "You are a CI orchestration advisor. You receive a JSON snapshot of a "
    "running CI workflow: the event, every job and its status, and the jobs "
    "that just became terminal this turn. Failed jobs carry a `result` digest "
    "with their failing sub-results and `links` (URLs of logs/artifacts the "
    "run produced).\n\n"
    "When a job has failed, investigate before deciding:\n"
    "  - use `fetch_log` to read the logs referenced in those `links` (only "
    "URLs present in the observation can be fetched; prefer `grep` to find the "
    "failing lines in large logs);\n"
    "  - use `grep_repo` to locate code in the checked-out PR (a symbol, an "
    "error string, a filename) and `read_file` to read the relevant source. "
    "Use these to confirm whether the failure is a real code defect.\n\n"
    "Then choose exactly ONE action for the run:\n"
    "  - \"continue\": nothing is wrong, or the failure is flaky/expected/"
    "unrelated — let the run proceed.\n"
    "  - \"cancel_run\": a failure makes the rest of the run pointless or "
    "unsafe (e.g. a fundamental build break that every downstream job depends "
    "on); stop the whole run.\n"
    "  - \"cancel_and_patch\": the root cause is a clear code defect you can fix "
    "as exact text replacements. Cancel the run and apply the fix, which is "
    "committed and pushed to the PR branch, triggering a fresh run. Include an "
    "\"edits\" array on this decision item (see below). Only choose this when you "
    "are confident the edits fully fix the failure.\n\n"
    "For \"cancel_and_patch\", read every file you edit with `read_file` first so "
    "each \"search\" string matches the file byte-for-byte, and give edits as:\n"
    '    "edits": [{"path": "<repo-relative file>", '
    '"search": "<exact snippet occurring once in the file>", '
    '"replace": "<replacement text>"}]\n'
    "Keep each `search` minimal but unique (it must occur exactly once in the "
    "file); use several edits for several sites.\n\n"
    "Respond with ONLY a JSON object (no prose, no markdown fences) of the form:\n"
    '{"reasoning": "<one short paragraph>", '
    '"root_cause": "<root cause, or empty if none/unknown>", '
    '"decision": [{"type": "<action>", "detail": "<what and why>", '
    '"edits": [ ... only for cancel_and_patch ... ]}]}\n'
    'Use type "continue" when there is nothing to act on.'
)

# `decision` items keep the {type, detail} shape the mock and the (future)
# action dispatcher share. We ask for JSON in the prompt rather than using
# `output_config.format` so the same code path works on the first-party API and
# on Amazon Bedrock (whose Messages endpoint rejects `output_config`).

# Tool the model can call to read a log/artifact referenced in the observation.
# `url` is validated against the per-call allowlist of observation links, so the
# model can only fetch what the run actually published (no SSRF).
_FETCH_LOG_TOOL = {
    "name": "fetch_log",
    "description": (
        "Fetch a CI log or artifact by URL to investigate a failure. The "
        "URL must be one of the `links` present in the observation. With "
        "`grep`, returns only matching lines (case-insensitive) with a few "
        "lines of context — best for finding errors in large logs. Without "
        "`grep`, returns a capped slice (the tail by default, where "
        "tracebacks usually are)."
    ),
    "input_schema": {
        "type": "object",
        "properties": {
            "url": {
                "type": "string",
                "description": "A log/artifact URL taken from the observation's links.",
            },
            "grep": {
                "type": "string",
                "description": "Optional case-insensitive substring; return only matching lines + context.",
            },
            "max_bytes": {
                "type": "integer",
                "description": "Optional cap on returned characters (default 20000).",
            },
            "from_end": {
                "type": "boolean",
                "description": "When not grepping, return the tail (true, default) or head (false).",
            },
        },
        "required": ["url"],
    },
}

# Tools for reading the checked-out PR source. Both are rooted at the repo
# working tree (the orchestrator runs from the clone) and refuse paths that
# escape it, so the model can read project code but not arbitrary host files.
_REPO_TOOLS = [
    {
        "name": "grep_repo",
        "description": (
            "Search the checked-out PR source for a pattern (git grep, basic "
            "regex) to locate code — a symbol, an error string, a filename. "
            "Returns `path:line: text` matches, capped. Use it to find the "
            "file behind a failure before read_file."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "pattern": {"type": "string", "description": "git-grep pattern (basic regex)."},
                "ignore_case": {"type": "boolean", "description": "Case-insensitive match (default false)."},
            },
            "required": ["pattern"],
        },
    },
    {
        "name": "read_file",
        "description": (
            "Read a source file from the checked-out PR by repo-relative path. "
            "Optionally restrict to a line range. Returns line-numbered text, "
            "capped. Paths that escape the repository are rejected."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "path": {"type": "string", "description": "Repo-relative file path, e.g. 'praktika/runner.py'."},
                "start_line": {"type": "integer", "description": "1-based first line to return (default 1)."},
                "max_lines": {"type": "integer", "description": "Max lines to return (default to EOF / cap)."},
            },
            "required": ["path"],
        },
    },
]

# Stop runaway tool loops: at most this many tool rounds before we force a
# final answer from whatever the model has seen.
_MAX_TOOL_ROUNDS = 8
# Network + size guards for log fetches.
_LOG_FETCH_TIMEOUT_S = 15
_LOG_HARD_MAX_BYTES = 200_000  # never pull more than this from S3 per fetch
_LOG_DEFAULT_RETURN_BYTES = 20_000  # default slice handed back to the model
_GREP_CONTEXT_LINES = 2
_GREP_MAX_LINES = 300
# Repo-read guards.
_REPO_READ_MAX_BYTES = 60_000
_REPO_READ_MAX_LINES = 1_000
_REPO_GREP_MAX_RESULTS = 200
_REPO_GREP_TIMEOUT_S = 20


def _repo_root():
    """Repository working tree the advisor may read — the orchestrator's CWD,
    which is the cloned PR checkout."""
    return os.getcwd()


def _safe_repo_path(rel):
    """Resolve a repo-relative path and confirm it stays inside the repo root
    (after following symlinks). Returns the absolute path, or None on escape."""
    root = os.path.realpath(_repo_root())
    target = os.path.realpath(os.path.join(root, rel or ""))
    if target == root or target.startswith(root + os.sep):
        return target
    return None


def _read_file(path, start_line=None, max_lines=None):
    """Read a repo file (line-numbered, capped) or return an error string."""
    target = _safe_repo_path(path)
    if target is None:
        return "error: path escapes the repository root"
    if not os.path.isfile(target):
        return f"error: not a file: {path}"
    try:
        with open(target, "r", encoding="utf-8", errors="replace") as f:
            lines = f.readlines()
    except Exception as e:
        return f"error reading {path}: {type(e).__name__}: {e}"
    try:
        start = max(1, int(start_line)) if start_line else 1
    except (TypeError, ValueError):
        start = 1
    try:
        count = int(max_lines) if max_lines else _REPO_READ_MAX_LINES
    except (TypeError, ValueError):
        count = _REPO_READ_MAX_LINES
    count = min(count, _REPO_READ_MAX_LINES)
    chunk = lines[start - 1 : start - 1 + count]
    numbered, size = [], 0
    for idx, line in enumerate(chunk, start=start):
        numbered.append(f"{idx}: {line.rstrip(os.linesep)}")
        size += len(line)
        if size >= _REPO_READ_MAX_BYTES:
            numbered.append("…(truncated)…")
            break
    return "\n".join(numbered) if numbered else "(empty range)"


def _grep_repo(pattern, ignore_case=False):
    """git-grep the repo for `pattern`; return capped `path:line: text` hits."""
    import subprocess

    if not pattern:
        return "error: empty pattern"
    cmd = ["git", "-C", _repo_root(), "grep", "-n", "-I", "--no-color"]
    if ignore_case:
        cmd.append("-i")
    cmd += ["-e", pattern]
    try:
        proc = subprocess.run(
            cmd, capture_output=True, text=True, timeout=_REPO_GREP_TIMEOUT_S
        )
    except Exception as e:
        return f"error running git grep: {type(e).__name__}: {e}"
    out = (proc.stdout or "").splitlines()
    if not out:
        return f"(no matches for {pattern!r})"
    capped = out[:_REPO_GREP_MAX_RESULTS]
    if len(out) > _REPO_GREP_MAX_RESULTS:
        capped.append(f"…({len(out) - _REPO_GREP_MAX_RESULTS} more matches truncated)…")
    return "\n".join(capped)


def _collect_log_urls(observation):
    """Set of fetchable log URLs from an observation's failed-job digests.

    The model may only fetch URLs that appear here — the allowlist that keeps
    fetch_log from reaching anything the run didn't publish.
    """
    urls = set()
    for entry in getattr(observation, "changed", None) or []:
        digest = entry.get("result") if isinstance(entry, dict) else None
        if not isinstance(digest, dict):
            continue
        for u in digest.get("links") or []:
            urls.add(u)
        for f in digest.get("failed") or []:
            if isinstance(f, dict):
                for u in f.get("links") or []:
                    urls.add(u)
    return urls


def _grep_log(text, pattern):
    """Return lines matching `pattern` (case-insensitive substring) with
    context, capped and with `--` separators between non-adjacent hunks."""
    lines = text.splitlines()
    needle = pattern.lower()
    keep = set()
    for i, line in enumerate(lines):
        if needle in line.lower():
            for j in range(max(0, i - _GREP_CONTEXT_LINES),
                           min(len(lines), i + _GREP_CONTEXT_LINES + 1)):
                keep.add(j)
    if not keep:
        return f"(no lines matching {pattern!r})"
    out = []
    prev = None
    for i in sorted(keep):
        if prev is not None and i != prev + 1:
            out.append("--")
        out.append(f"{i + 1}: {lines[i]}")
        prev = i
        if len(out) >= _GREP_MAX_LINES:
            out.append("…(more matches truncated)…")
            break
    return "\n".join(out)


def _fetch_log(url, grep=None, max_bytes=_LOG_DEFAULT_RETURN_BYTES, from_end=True):
    """HTTP GET a log/artifact and return a bounded, optionally grepped slice."""
    import requests

    resp = requests.get(url, timeout=_LOG_FETCH_TIMEOUT_S, stream=True)
    resp.raise_for_status()
    chunks, total = [], 0
    for chunk in resp.iter_content(chunk_size=8192):
        if not chunk:
            continue
        chunks.append(chunk)
        total += len(chunk)
        if total >= _LOG_HARD_MAX_BYTES:
            break
    text = b"".join(chunks).decode("utf-8", "replace")
    if grep:
        return _grep_log(text, grep)
    try:
        cap = min(int(max_bytes or _LOG_DEFAULT_RETURN_BYTES), _LOG_HARD_MAX_BYTES)
    except (TypeError, ValueError):
        cap = _LOG_DEFAULT_RETURN_BYTES
    if len(text) <= cap:
        return text
    if from_end:
        return "…(head truncated)…\n" + text[-cap:]
    return text[:cap] + "\n…(tail truncated)…"


def _execute_tool(name, tool_input, allowed_urls):
    """Run one tool call; always returns a string (never raises) so a bad
    call becomes feedback to the model rather than a crashed turn."""
    if not isinstance(tool_input, dict):
        return "error: invalid tool input"
    try:
        if name == "fetch_log":
            url = tool_input.get("url", "")
            if url not in allowed_urls:
                return (
                    "error: url is not in the allowed set for this observation; "
                    "only fetch URLs listed under a failed job's `links`."
                )
            return _fetch_log(
                url,
                grep=tool_input.get("grep") or None,
                max_bytes=tool_input.get("max_bytes") or _LOG_DEFAULT_RETURN_BYTES,
                from_end=tool_input.get("from_end", True),
            )
        if name == "read_file":
            return _read_file(
                tool_input.get("path", ""),
                start_line=tool_input.get("start_line"),
                max_lines=tool_input.get("max_lines"),
            )
        if name == "grep_repo":
            return _grep_repo(
                tool_input.get("pattern", ""),
                ignore_case=bool(tool_input.get("ignore_case", False)),
            )
    except Exception as e:
        return f"error in tool {name!r}: {type(e).__name__}: {e}"
    return f"error: unknown tool {name!r}"


def _parse(text):
    """Lenient parse of the model's reply into (reasoning, decision).

    Tolerates markdown fences and leading/trailing prose: tries the whole string
    as JSON, then the first ``{...}`` block. Falls back to treating the raw text
    as the reasoning with an empty decision, so a non-JSON reply never crashes
    the turn. A non-empty ``root_cause`` is folded into the reasoning so it lands
    in the recorded trace without changing the Turn schema.
    """
    raw = (text or "").strip()
    candidate = raw
    if candidate.startswith("```"):
        # strip ```json ... ``` fences
        candidate = re.sub(r"^```[a-zA-Z]*\n?", "", candidate)
        candidate = re.sub(r"\n?```$", "", candidate).strip()

    data = None
    for attempt in (candidate, _first_object(candidate)):
        if not attempt:
            continue
        try:
            data = json.loads(attempt)
            break
        except Exception:
            continue

    if not isinstance(data, dict):
        return raw, []  # not JSON — keep the text as reasoning
    decision = data.get("decision")
    if not isinstance(decision, list):
        decision = []
    reasoning = data.get("reasoning", "")
    root_cause = data.get("root_cause")
    if isinstance(root_cause, str) and root_cause.strip():
        reasoning = (f"{reasoning}\n\nRoot cause: {root_cause.strip()}").strip()
    return reasoning, decision


def _first_object(text):
    m = re.search(r"\{.*\}", text or "", re.DOTALL)
    return m.group(0) if m else ""


def _accumulate_usage(totals, sdk_usage):
    """Add one API call's token counts into the running totals dict."""
    totals["input"] += getattr(sdk_usage, "input_tokens", 0) or 0
    totals["output"] += getattr(sdk_usage, "output_tokens", 0) or 0
    totals["cache_read"] += getattr(sdk_usage, "cache_read_input_tokens", 0) or 0
    totals["cache_write"] += getattr(sdk_usage, "cache_creation_input_tokens", 0) or 0


class AnthropicProvider(AIProvider):
    name = "anthropic"
    DEFAULT_MODEL = "claude-opus-4-8"

    def __init__(self, model=""):
        super().__init__(model=model)
        self._client = None  # lazily constructed on first decide()

    def _get_client(self):
        if self._client is None:
            try:
                import anthropic
            except ImportError as e:  # optional dependency
                raise RuntimeError(
                    "anthropic SDK not installed; `pip install anthropic` to use "
                    "AI_PROVIDER='anthropic'"
                ) from e
            # Resolves credentials from ANTHROPIC_API_KEY (or an `ant` profile).
            self._client = anthropic.Anthropic()
        return self._client

    def on_job_failure(self, observation) -> Turn:
        # Pure model logic — observation/turn tracking and the AI Advisor check
        # are handled by AIProvider.consult, which brackets this call.
        client = self._get_client()
        model = self.resolved_model()

        # Investigation toolset for this turn. This hook only fires on a
        # failure, so the repo-read tools are always offered; fetch_log is added
        # only when the observation carries log links (its allowlist).
        allowed_urls = _collect_log_urls(observation)
        tools = list(_REPO_TOOLS)
        if allowed_urls:
            tools.append(_FETCH_LOG_TOOL)
        messages = [
            {"role": "user", "content": json.dumps(observation.to_dict(), indent=2)}
        ]
        totals = {"input": 0, "output": 0, "cache_read": 0, "cache_write": 0}
        tool_calls = 0

        t0 = time.time()
        text = ""
        for _ in range(_MAX_TOOL_ROUNDS + 1):
            kwargs = dict(
                model=model,
                max_tokens=4000,
                system=_SYSTEM,
                messages=messages,
            )
            if tools:
                kwargs["tools"] = tools
            resp = client.messages.create(**kwargs)
            _accumulate_usage(totals, resp.usage)

            content = list(resp.content or [])
            text = next(
                (b.text for b in content if getattr(b, "type", None) == "text"),
                text,
            )
            tool_uses = [b for b in content if getattr(b, "type", None) == "tool_use"]
            if not tool_uses:
                break

            # Feed the assistant's tool_use turn back, then answer each call.
            messages.append({"role": "assistant", "content": content})
            results = []
            for tu in tool_uses:
                tool_calls += 1
                out = _execute_tool(tu.name, tu.input, allowed_urls)
                results.append(
                    {
                        "type": "tool_result",
                        "tool_use_id": tu.id,
                        "content": out,
                    }
                )
            messages.append({"role": "user", "content": results})
        latency_ms = int((time.time() - t0) * 1000)

        reasoning, decision = _parse(text)
        usage = self._usage(model, totals, latency_ms)
        print(
            f"[AI {self.name}] on_job_failure: model={model} "
            f"decision={[d.get('type') for d in decision if isinstance(d, dict)]} "
            f"tool_calls={tool_calls} "
            f"tokens={usage.input_tokens}/{usage.output_tokens} "
            f"cost=${usage.cost_usd:.4f}"
        )
        return Turn(reasoning=reasoning, decision=decision, usage=usage)

    def _usage(self, model, totals, latency_ms) -> Usage:
        inp = totals["input"]
        out = totals["output"]
        cache_read = totals["cache_read"]
        cache_write = totals["cache_write"]

        in_price, out_price = _price_per_mtok(model)
        cost = (
            inp * in_price
            + cache_write * in_price * 1.25  # cache write premium
            + cache_read * in_price * 0.1  # cache read discount
            + out * out_price
        ) / 1_000_000

        return Usage(
            input_tokens=inp + cache_read + cache_write,
            output_tokens=out,
            cost_usd=round(cost, 6),
            latency_ms=latency_ms,
            provider=self.name,
            model=model,
        )


class BedrockProvider(AnthropicProvider):
    """Same provider, served through Amazon Bedrock instead of the first-party API.

    Only the transport differs: the Bedrock Runtime client authenticates with the
    standard AWS credential chain (env / shared profile / instance role — no
    ``ANTHROPIC_API_KEY``). Runtime targets may be raw Bedrock foundation-model
    ids or Bedrock inference-profile ids such as ``eu.anthropic.*`` /
    ``global.anthropic.*``. The prompt, structured-output schema, and
    decode/usage logic are inherited.

    Region resolution: explicit ``aws_region`` arg → ``Settings.AWS_REGION`` →
    ``AWS_REGION`` / ``AWS_DEFAULT_REGION`` env. Bedrock Runtime has no region
    fallback, so a region must be resolvable or ``decide`` raises (→ advisor
    error Turn).
    """

    name = "bedrock"
    DEFAULT_MODEL = "global.anthropic.claude-opus-4-8"

    def __init__(self, model="", aws_region=""):
        super().__init__(model=model)
        self.aws_region = aws_region or ""

    def _region(self):
        if self.aws_region:
            return self.aws_region
        import os

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
                from anthropic import AnthropicBedrock
            except ImportError as e:  # optional dependency
                raise RuntimeError(
                    "anthropic[bedrock] not installed; `pip install 'anthropic[bedrock]'` "
                    "to use AI_PROVIDER='bedrock'"
                ) from e
            region = self._region()
            if not region:
                raise RuntimeError(
                    "no AWS region for Bedrock; set Settings.AWS_REGION or AWS_REGION"
                )
            self._client = AnthropicBedrock(aws_region=region)
        return self._client
