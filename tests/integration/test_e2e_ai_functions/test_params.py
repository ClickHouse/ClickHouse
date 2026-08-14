"""Suite A2: does the endpoint honor the parameters and settings we send?

These are the cases that justify hitting a real endpoint at all - a mock honors everything
by construction. Findings #1 (the gateway silently drops `response_format: json_schema`)
and #3 (`max_tokens <= 0` accepted) from an earlier gateway audit are exactly this class of
bug. Note that #1 no longer reproduces - see A2-11 below.

Everything endpoint-independent - quota skip and throw, `ai_function_throw_on_error`,
embedding batch counts, timeout and retry behaviour - is left to the free mock suite in
`tests/integration/test_ai_functions/`, where it is exact rather than flaky.

Cases whose outcome the endpoint or model decides, not ClickHouse, are report-only: they
record what happened so a capability change shows up as a diff between runs.
"""

import json

import pytest

from . import corpus as ai_corpus
from .asserts import Report, cosine, parse_json_rows
from .conftest import load_table, requires_capable_model, requires_live_endpoint

pytestmark = [pytest.mark.e2e, requires_live_endpoint]

CHAT = "map('credentials','ai_e2e_chat')"
CATEGORIES = "['positive','negative','neutral']"
LONG_PROMPT = "Write 400 words about columnar storage."


@pytest.fixture(scope="module")
def a2_tables(instance):
    load_table(
        instance,
        "a2_embed",
        [("id", "UInt32"), ("text", "String")],
        ai_corpus.embed_bulk()[:12],
    )
    load_table(
        instance,
        "a2_pairs",
        [
            ("id", "UInt32"),
            ("anchor", "String"),
            ("paraphrase", "String"),
            ("unrelated", "String"),
        ],
        ai_corpus.embed_pairs()[:3],
    )
    load_table(
        instance,
        "a2_one",
        [("id", "UInt32"), ("text", "String")],
        [{"id": 0, "text": LONG_PROMPT}],
    )
    yield
    for table in ("a2_embed", "a2_pairs", "a2_one"):
        instance.query(f"DROP TABLE IF EXISTS {table} SYNC")


@pytest.fixture(scope="module")
def report(cfg):
    report = Report(
        "suite_a2",
        {
            "target": cfg.target.name,
            "chat_model": cfg.chat_model,
            "embed_dim_model": cfg.embed_dim_model,
        },
    )
    yield report
    report.flush()
    print("\n" + report.render(["case", "detail", "verdict"]))


def _embed(model, extra=""):
    params = "'credentials','ai_e2e_embed'"
    if extra:
        params += f",{extra}"
    return f"aiEmbed(text, '{model}', map({params}))"


# ---------------------------------------------------------------------------
# A2-1 / A2-2 / A2-3 dimensions
# ---------------------------------------------------------------------------


@requires_capable_model
@pytest.mark.parametrize("dimensions", [256, 512, 1024])
def test_a2_1_dimensions_honored(q, cfg, a2_tables, report, dimensions):
    expression = _embed(cfg.embed_dim_model, f"'dimensions','{dimensions}'")
    result, events = q.run(
        f"SELECT id, {expression} AS vec "
        f"FROM a2_embed ORDER BY id FORMAT JSONEachRow",
        case=f"a2_1_dim{dimensions}",
        counting=True,
        rows=12,
    )
    parsed = parse_json_rows(result)
    sizes = {len(row["vec"]) for row in parsed}
    report.add(
        f"a2_1 dimensions={dimensions}",
        detail=f"sizes={sorted(sizes)} api_calls={events['api_calls']}",
        verdict="ok" if sizes == {dimensions} else "NOT honored",
    )
    assert sizes == {dimensions}, (
        f"requested {dimensions} dimensions, got {sorted(sizes)}"
    )


def test_a2_2_dimensions_on_native_only_model(q, cfg, a2_tables, report):
    """Report-only: whether the endpoint errors, ignores, or honors `dimensions` on a
    model that has no reduction support. That is an endpoint capability, not a ClickHouse
    contract, so it is never a hard failure."""
    expression = _embed(cfg.embed_model, "'dimensions','256'")
    sql = (
        f"SELECT id, {expression} AS vec "
        f"FROM a2_embed ORDER BY id LIMIT 2 FORMAT JSONEachRow"
    )
    try:
        result, _events = q.run(sql, case="a2_2_native_dim", counting=True, rows=2)
        sizes = sorted({len(row["vec"]) for row in parse_json_rows(result)})
        verdict = "honored" if sizes == [256] else f"ignored (sizes={sizes})"
    except Exception as error:  # an endpoint may legitimately reject the parameter
        verdict = f"rejected: {str(error)[:120]}"
    report.add("a2_2 dimensions on native-only model", detail=cfg.embed_model, verdict=verdict)
    print(f"\n[ai-e2e] dimensions on {cfg.embed_model}: {verdict}")


@requires_capable_model
def test_a2_3_dimensions_in_similarity(q, cfg, a2_tables, report):
    model = cfg.embed_dim_model
    result, _events = q.run(
        f"SELECT id, "
        f"aiSimilarity(anchor, paraphrase, '{model}', "
        f"map('credentials','ai_e2e_embed','dimensions','256')) AS close, "
        f"aiSimilarity(anchor, unrelated, '{model}', "
        f"map('credentials','ai_e2e_embed','dimensions','256')) AS far "
        f"FROM a2_pairs ORDER BY id FORMAT JSONEachRow",
        case="a2_3_similarity_dim",
        counting=True,
        rows=3,
    )
    parsed = parse_json_rows(result)
    report.add(
        "a2_3 dimensions in aiSimilarity",
        detail=f"pairs={[(r['close'], r['far']) for r in parsed]}",
        verdict="ok",
    )
    for row in parsed:
        assert -1.001 <= row["close"] <= 1.001 and -1.001 <= row["far"] <= 1.001
        assert row["close"] > row["far"], (
            f"reduced-dimension ordering broke for row {row['id']}: "
            f"{row['close']} vs {row['far']}"
        )


# ---------------------------------------------------------------------------
# A2-4 max_tokens
# ---------------------------------------------------------------------------


def test_a2_4_max_tokens_limits_output(q, cfg, a2_tables, report):
    lengths = {}
    reported = {}
    for limit in (16, 256):
        result, events = q.run(
            f"SELECT aiGenerate(text, map('credentials','ai_e2e_chat',"
            f"'max_tokens','{limit}')) AS out FROM a2_one FORMAT JSONEachRow",
            case=f"a2_4_max_tokens_{limit}",
            counting=True,
            rows=1,
        )
        out = parse_json_rows(result)[0]["out"]
        lengths[limit] = len(out)
        reported[limit] = events["output_tokens"]

    within = reported[16] <= 16 * 1.1 if reported[16] else None
    report.add(
        "a2_4 max_tokens",
        detail=f"chars={lengths} reported_out_tokens={reported}",
        verdict="ok" if lengths[16] < lengths[256] else "NOT honored",
    )
    # Asserted: a small limit must produce visibly less text than a large one.
    assert lengths[16] > 0 and lengths[256] > 0, "empty completion"
    assert lengths[16] < lengths[256], (
        f"max_tokens did not shorten the output: {lengths}"
    )
    # Reported token count is recorded, not asserted: reasoning-capable routes can bill
    # more completion tokens than `max_tokens`, and the gateway's handling is loose.
    print(f"\n[ai-e2e] max_tokens: chars={lengths} reported={reported} within_16={within}")


# ---------------------------------------------------------------------------
# A2-5 temperature 0 determinism (report-only)
# ---------------------------------------------------------------------------


def test_a2_5_temperature_zero_determinism(q, cfg, a2_tables, report):
    """Report-only: `OpenAIProvider` sends no `seed` and the gateway fronts several
    backends, so byte-identical output at `temperature = 0` is not a contract."""
    outputs = []
    for attempt in range(3):
        result, _events = q.run(
            f"SELECT aiGenerate('Name the capital of France. Answer with one word.', "
            f"map('credentials','ai_e2e_chat','temperature','0')) AS out "
            f"FROM numbers(1) FORMAT JSONEachRow",
            case=f"a2_5_temp0_{attempt}",
            counting=True,
            rows=1,
        )
        outputs.append(parse_json_rows(result)[0]["out"].strip())
    identical = len(set(outputs)) == 1
    report.add(
        "a2_5 temperature=0 determinism",
        detail=f"{len(set(outputs))} distinct of 3",
        verdict="deterministic" if identical else "NOT deterministic",
    )
    print(f"\n[ai-e2e] temperature=0: {'identical' if identical else set(outputs)}")


# ---------------------------------------------------------------------------
# A2-6 model override reaches the wire
# ---------------------------------------------------------------------------


def test_a2_6_bogus_model_is_rejected(q, cfg, a2_tables, report):
    error = q.error(
        "SELECT aiGenerate('hello', map('credentials','ai_e2e_chat',"
        "'model','definitely-not-a-model')) FROM numbers(1)",
        case="a2_6_bogus_model",
    )
    assert error, "a nonexistent model must not silently succeed"
    mentioned = "definitely-not-a-model" in error
    report.add(
        "a2_6 bogus model override",
        detail="names the model" if mentioned else "generic error",
        verdict="ok",
    )
    print(f"\n[ai-e2e] bogus model error: {error[:200]}")


def test_a2_7_model_override_precedence(q, cfg, a2_tables, report):
    if not cfg.chat_model_alt:
        pytest.skip("AI_E2E_CHAT_MODEL_ALT is not set")
    result, events = q.run(
        f"SELECT aiGenerate(text, map('credentials','ai_e2e_chat',"
        f"'model','{cfg.chat_model_alt}')) AS out FROM a2_one FORMAT JSONEachRow",
        case="a2_7_model_alt",
        counting=True,
        rows=1,
    )
    out = parse_json_rows(result)[0]["out"]
    assert out.strip(), "override to a valid model must succeed"
    report.add(
        "a2_7 model override precedence",
        detail=f"{cfg.chat_model_alt} -> {len(out)} chars",
        verdict="ok",
    )


# ---------------------------------------------------------------------------
# A2-8 / A2-9 function-specific parameters
# ---------------------------------------------------------------------------


@requires_capable_model
def test_a2_8_system_prompt(q, cfg, a2_tables, report):
    result, _events = q.run(
        "SELECT aiGenerate('Say something.', map('credentials','ai_e2e_chat',"
        "'system_prompt','Reply with exactly the word BANANA and nothing else.')) AS out "
        "FROM numbers(1) FORMAT JSONEachRow",
        case="a2_8_system_prompt",
        counting=True,
        rows=1,
    )
    out = parse_json_rows(result)[0]["out"]
    honored = "BANANA" in out.upper()
    report.add(
        "a2_8 system_prompt", detail=out[:60], verdict="ok" if honored else "ignored"
    )
    assert honored, f"system_prompt was not applied: {out[:120]}"


@requires_capable_model
def test_a2_9_translate_instructions(q, cfg, a2_tables, report):
    row = ai_corpus.translate()[0]
    result, _events = q.run(
        f"SELECT aiTranslate('{row['text']}', '{ai_corpus.TRANSLATE_TARGET}', "
        f"map('credentials','ai_e2e_chat','instructions','Prefix your answer with \"TR:\".')) "
        f"AS out FROM numbers(1) FORMAT JSONEachRow",
        case="a2_9_instructions",
        counting=True,
        rows=1,
    )
    out = parse_json_rows(result)[0]["out"].strip()
    honored = out.startswith("TR:")
    report.add(
        "a2_9 aiTranslate instructions",
        detail=out[:60],
        verdict="ok" if honored else "ignored",
    )
    assert honored, f"instructions were not applied: {out[:120]}"


# ---------------------------------------------------------------------------
# A2-10 embedding batch size: counts, and that batching does not change vectors
# ---------------------------------------------------------------------------


def test_a2_10_batch_size_consistency(q, cfg, a2_tables, report):
    vectors = {}
    for batch in (1, 3, 100):
        result, events = q.run(
            f"SELECT id, {_embed(cfg.embed_model)} AS vec FROM a2_embed "
            f"ORDER BY id FORMAT JSONEachRow",
            case=f"a2_10_batch{batch}",
            counting=True,
            rows=12,
            settings={"ai_function_embedding_max_batch_size": batch},
        )
        parsed = parse_json_rows(result)
        expected_calls = (12 + batch - 1) // batch
        assert events["api_calls"] == expected_calls, (
            f"batch={batch}: AIAPICalls={events['api_calls']}, expected {expected_calls}"
        )
        vectors[batch] = [row["vec"] for row in parsed]

    reference = vectors[100]
    worst = 0.0
    for batch, batched in vectors.items():
        for index, (left, right) in enumerate(zip(reference, batched)):
            worst = max(worst, 1 - cosine(left, right))
    report.add(
        "a2_10 batch size consistency",
        detail=f"worst 1-cos across batch sizes = {worst:.2e}",
        verdict="ok" if worst <= 1e-4 else "NOT batch-invariant",
    )
    print(f"\n[ai-e2e] batch-size vector agreement: worst 1-cos = {worst:.2e}")
    assert worst <= 1e-4, (
        f"the same input embedded differently depending on batch size (worst 1-cos "
        f"{worst:.2e}); anything built on top of aiEmbed would inherit that"
    )


# ---------------------------------------------------------------------------
# A2-11 structured-output enforcement (report-only)
# ---------------------------------------------------------------------------


def test_a2_11_structured_output_enforcement(q, cfg, a2_tables, report):
    """Report-only, and the single most valuable endpoint-capability signal.

    A1-2 and A1-3 assert the user-visible contract, which passes whenever the model
    happens to comply. This pushes the model off-schema on purpose, so "schema enforced"
    and "prompt luck" become distinguishable.
    """
    gibberish = "qwx zzblorp fnord 8812 ~~~"
    result, _events = q.run(
        f"SELECT aiClassify('{gibberish}', {CATEGORIES}, {CHAT}) AS out "
        f"FROM numbers(1) FORMAT JSONEachRow",
        case="a2_11_offschema_classify",
        counting=True,
        rows=1,
    )
    label = parse_json_rows(result)[0]["out"].strip()
    inside = label in ai_corpus.CATEGORIES
    report.add(
        "a2_11 classify off-schema input",
        detail=f"returned {label!r}",
        verdict="stayed in enum" if inside else "LEAKED out-of-enum output",
    )

    unsatisfiable = '{"blood_type": "the blood type of the author", "isbn": "the ISBN"}'
    result, _events = q.run(
        f"SELECT aiExtract('The weather was mild yesterday.', '{unsatisfiable}', {CHAT}) "
        f"AS out FROM numbers(1) FORMAT JSONEachRow",
        case="a2_11_unsatisfiable_extract",
        counting=True,
        rows=1,
    )
    out = parse_json_rows(result)[0]["out"]
    try:
        keys = sorted(json.loads(out))
        verdict = (
            "kept the key set" if keys == ["blood_type", "isbn"] else f"keys={keys}"
        )
    except ValueError:
        verdict = f"not JSON: {out[:80]!r}"
    report.add("a2_11 extract unsatisfiable schema", detail=out[:80], verdict=verdict)
    print(f"\n[ai-e2e] schema enforcement: classify={'in-enum' if inside else 'LEAK'}, extract={verdict}")
