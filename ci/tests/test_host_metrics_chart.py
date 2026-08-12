"""Regression tests for the `addHostMetricsChart` renderer in ci/praktika/json.html.

json.html is the viewer for *archived* reports, so the chart code carries a
compatibility contract across host-metrics schema generations:

  * reports from before `iowait` was collected (`series` has no `iowait` key,
    `peaks` has no `iowait`) must render exactly as they did before the iowait
    line was added - same paths, same legend rows, same tooltip entries, and
    no trace of "iowait" anywhere in the produced DOM;
  * current reports render the extra `iowait` series as its own line, legend
    row and tooltip entry, drawn under the `cpu` line;
  * ancient reports (no exact `peaks` object) and empty payloads are skipped
    without rendering anything.

The tests extract the real `addHostMetricsChart` source out of json.html and
drive it inside a V8 context (`mini-racer`) under a minimal DOM shim, then
assert on the element tree it builds. A refactor of json.html that breaks any
of these contracts fails here instead of silently blanking or skewing old
charts.
"""

from pathlib import Path

import pytest
from py_mini_racer import MiniRacer

JSON_HTML = Path(__file__).resolve().parents[2] / "ci" / "praktika" / "json.html"

CPU_COLOR = "#4e79a7"
IOWAIT_COLOR = "#f28e2b"
MEM_COLOR = "#e15759"
DISK_COLOR = "#59a14f"

# Minimal DOM shim: just enough surface for addHostMetricsChart - element
# creation, tree building, attributes, styles and event listeners. Elements
# are plain objects so the test can walk and serialize the resulting tree.
DOM_SHIM = """
function makeElement(tag, ns) {
    return {
        tagName: tag,
        ns: ns || null,
        children: [],
        attrs: {},
        style: {},
        className: "",
        innerHTML: "",
        textContent: "",
        listeners: {},
        clientWidth: 320,
        offsetWidth: 60,
        appendChild(c) { this.children.push(c); return c; },
        setAttribute(k, v) { this.attrs[k] = String(v); },
        getAttribute(k) { return this.attrs[k]; },
        addEventListener(t, l) { (this.listeners[t] = this.listeners[t] || []).push(l); },
        removeEventListener() {},
        getBoundingClientRect() { return { left: 0, top: 0, width: 320, height: 130 }; },
    };
}

const statusContainer = makeElement("div");
const document = {
    body: makeElement("body"),
    getElementById: (id) => (id === "status-container" ? statusContainer : null),
    createElement: (tag) => makeElement(tag),
    createElementNS: (ns, tag) => makeElement(tag, ns),
};
const getComputedStyle = () => ({ getPropertyValue: () => "" });
const activeListeners = {
    listeners: [],
    add(element, type, listener) {
        element.addEventListener(type, listener);
        this.listeners.push({ element, type, listener });
        return listener;
    },
};

// Renders `metrics` into a fresh status container and returns a serializable
// summary of everything the compatibility contract cares about.
function render(metrics) {
    statusContainer.children = [];
    activeListeners.listeners = [];
    addHostMetricsChart(metrics);

    const all = [];
    (function collect(node) {
        all.push(node);
        node.children.forEach(collect);
    })(statusContainer);

    const paths = all
        .filter((n) => n.tagName === "path")
        .map((n) => ({ stroke: n.attrs["stroke"], opacity: n.attrs["stroke-opacity"], d: n.attrs["d"] }));
    // Legend rows are the innerHTML lines carrying the colour swatch.
    const legend = all
        .map((n) => n.innerHTML)
        .filter((h) => h && h.includes("■"));
    const texts = all
        .map((n) => `${n.innerHTML || ""} ${n.textContent || ""}`)
        .join("\\n");

    // Fire a synthetic hover in the middle of the plot and grab the tooltip.
    let tip = "";
    const svg = all.find((n) => n.tagName === "svg");
    if (svg && svg.listeners["mousemove"]) {
        svg.listeners["mousemove"][0]({ clientX: 160, clientY: 10 });
        const tipNode = all.find((n) => n.style.position === "absolute" && n.style.pointerEvents === "none");
        if (tipNode && tipNode.style.display !== "none") tip = tipNode.innerHTML;
    }

    return { appended: statusContainer.children.length, paths, legend, texts, tip };
}
"""


def extract_function(source, name):
    """Extract `function <name>(...) {...}` from JS source by brace matching."""
    marker = f"function {name}("
    start = source.index(marker)
    depth = 0
    for i in range(source.index("{", start), len(source)):
        if source[i] == "{":
            depth += 1
        elif source[i] == "}":
            depth -= 1
            if depth == 0:
                return source[start : i + 1]
    raise ValueError(f"unbalanced braces extracting {name}")


@pytest.fixture(scope="module")
def ctx():
    chart_source = extract_function(JSON_HTML.read_text(), "addHostMetricsChart")
    # Sanity-check the extraction grabbed the whole renderer.
    assert "statusContainer.appendChild(wrapper)" in chart_source
    racer = MiniRacer()
    racer.eval(DOM_SHIM)
    racer.eval(chart_source)
    return racer


def render(ctx, metrics):
    return ctx.call("render", metrics)


def series(*points):
    """Points are (t, avg, peak) triples, matching the collector output."""
    return [list(p) for p in points]


def old_payload():
    """A report captured before `iowait` was collected: no `series.iowait`,
    no `peaks.iowait`. Every archived report until 2026-08 looks like this."""
    return {
        "interval": 5,
        "duration": 300,
        "mem_total_gb": 61,
        "disk_total_gb": 590,
        "n_raw": 300,
        "peaks": {"cpu": 87.5, "mem": 42.1, "disk": 55.0},
        "psi": {"cpu_s": 1.2, "mem_some_s": 0, "io_some_s": 105.7},
        "series": {
            "cpu": series((0, 10, 20), (150, 80, 87.5), (300, 5, 9)),
            "mem": series((0, 30, 31), (150, 40, 42.1), (300, 35, 36)),
            "disk": series((0, 50, 50), (150, 52, 53), (300, 54, 55)),
        },
    }


def new_payload():
    """A report from the current collector: `iowait` alongside the other
    series, with `cpu + iowait <= 100` at every point (cpu excludes iowait)."""
    p = old_payload()
    p["peaks"]["iowait"] = 64.0
    p["series"]["iowait"] = series((0, 2, 4), (150, 12, 25), (300, 55, 64))
    return p


def test_pre_iowait_report_renders_as_before(ctx):
    """The exact rendering contract for archived pre-iowait reports: the same
    widget the pre-iowait chart code produced, with no trace of iowait."""
    r = render(ctx, old_payload())
    assert r["appended"] == 1
    assert "iowait" not in r["texts"]

    # One faint peak envelope + one solid average line per metric, drawn
    # disk, mem, cpu (cpu on top), and no path in the iowait colour.
    strokes = [p["stroke"] for p in r["paths"]]
    assert strokes == [DISK_COLOR] * 2 + [MEM_COLOR] * 2 + [CPU_COLOR] * 2
    assert [p["opacity"] for p in r["paths"]] == ["0.35", "1"] * 3

    # Legend: cpu / ram / disk rows only.
    assert len(r["legend"]) == 3
    assert "cpu 88% peak" in r["legend"][0]
    assert "ram 42% of 61GB" in r["legend"][1]
    assert "disk 55% of 590GB" in r["legend"][2]

    # Tooltip at t=150s: cpu, ram and disk entries, nothing else.
    assert "cpu</span> 80%" in r["tip"]
    assert "ram</span> 40%" in r["tip"]
    assert "disk</span> 52%" in r["tip"]


def test_current_report_renders_iowait_line(ctx):
    r = render(ctx, new_payload())
    assert r["appended"] == 1

    # The iowait envelope + average line slot in under the cpu line.
    strokes = [p["stroke"] for p in r["paths"]]
    assert strokes == (
        [DISK_COLOR] * 2 + [MEM_COLOR] * 2 + [IOWAIT_COLOR] * 2 + [CPU_COLOR] * 2
    )

    # Legend gains an iowait row right below cpu.
    assert len(r["legend"]) == 4
    assert "cpu 88% peak" in r["legend"][0]
    assert "iowait 64% peak" in r["legend"][1]
    assert IOWAIT_COLOR in r["legend"][1]

    # Tooltip at t=150s picks up the iowait point (avg 12, peak 25).
    assert "iowait</span> 12%" in r["tip"]
    assert "(peak 25%)" in r["tip"]


def test_iowait_series_without_peak_gets_no_legend_row(ctx):
    """A defensive shape: the series is drawn but the legend row (which needs
    `peaks.iowait`) is skipped rather than rendered as `NaN`."""
    p = new_payload()
    del p["peaks"]["iowait"]
    r = render(ctx, p)
    assert r["appended"] == 1
    assert [q["stroke"] for q in r["paths"]].count(IOWAIT_COLOR) == 2
    assert len(r["legend"]) == 3
    assert "NaN" not in r["texts"]
    assert "iowait" not in "".join(r["legend"])


def test_ancient_and_empty_payloads_are_skipped(ctx):
    # Pre-`peaks` schema ([t, v] points): skipped, not half-rendered.
    assert (
        render(
            ctx,
            {"interval": 5, "duration": 60, "series": {"cpu": [[0, 1], [60, 2]]}},
        )["appended"]
        == 0
    )
    assert render(ctx, None)["appended"] == 0
    assert render(ctx, {})["appended"] == 0
    # Too few points to draw a line.
    assert (
        render(ctx, {"peaks": {"cpu": 1}, "series": {"cpu": series((0, 1, 1))}})[
            "appended"
        ]
        == 0
    )
