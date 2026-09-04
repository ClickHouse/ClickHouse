// Painting the search term where it occurs on screen.
//
// The viewer renders into shadow roots and lays out only the lines around the
// scroll position, so there is nothing to mark up ahead of time and nothing to
// mark up outside the window. This paints what is rendered, and is called again
// whenever that changes — a jump, a scroll, a file switch.
//
// Nothing in the viewer's DOM is modified: the CSS Custom Highlight API takes
// ranges over text nodes and paints them, so the next re-render simply drops
// them rather than leaving wrapper elements behind. `::highlight()` is scoped to
// the tree the text lives in, so the rule is adopted into each shadow root as
// the walk meets it. Where the API is missing the search still works — the line
// it lands on is selected — and this paints nothing.

const NAME = 'diff-review-find';
/// A cap on ranges, not on matches: a one-letter query over a rendered window is
/// thousands of them, and past a point the page is yellow rather than marked.
const MAX_RANGES = 2000;

const RULE =
  `::highlight(${NAME}) {` +
  ' background-color: light-dark(#ffe08a, #8a6100);' +
  ' color: light-dark(#111, #fff);' +
  ' border-radius: 2px; }';

const supported = () => typeof CSS !== 'undefined' && CSS.highlights != null && typeof Highlight === 'function';

let sheet = null;
const styled = new WeakSet();

/// The rule has to be in the same tree as the text it paints, and the viewer
/// builds those trees as it renders, so each one is styled when it is first met.
function ensureStyle(root) {
  if (styled.has(root)) return;
  styled.add(root);
  try {
    sheet ??= (() => {
      const made = new CSSStyleSheet();
      made.replaceSync(RULE);
      return made;
    })();
    root.adoptedStyleSheets = [...root.adoptedStyleSheets, sheet];
  } catch {
    // A tree that will not take the sheet just goes unpainted.
  }
}

/// Every text node under `root`, descending through open shadow roots — which is
/// where all of the code is.
function* textNodes(root) {
  const walker = document.createTreeWalker(root, NodeFilter.SHOW_ELEMENT | NodeFilter.SHOW_TEXT);
  for (let node = walker.nextNode(); node != null; node = walker.nextNode()) {
    if (node.nodeType === Node.TEXT_NODE) {
      if (node.data.trim() !== '') yield node;
    } else if (node.shadowRoot != null) {
      ensureStyle(node.shadowRoot);
      yield* textNodes(node.shadowRoot);
    }
  }
}

/// Marks every occurrence of `query` in what `root` currently has rendered.
/// Returns how many were painted, which is a property of the window on screen
/// and not of the file — the count in the find bar comes from the matcher.
export function paintMatches(root, query) {
  if (!supported()) return 0;
  CSS.highlights.delete(NAME);

  const needle = query.trim().toLowerCase();
  if (root == null || needle === '') return 0;
  ensureStyle(document);

  const ranges = [];
  for (const node of textNodes(root)) {
    const haystack = node.data.toLowerCase();
    for (let at = haystack.indexOf(needle); at !== -1; at = haystack.indexOf(needle, at + needle.length)) {
      const range = document.createRange();
      range.setStart(node, at);
      range.setEnd(node, at + needle.length);
      ranges.push(range);
      if (ranges.length >= MAX_RANGES) break;
    }
    if (ranges.length >= MAX_RANGES) break;
  }

  if (ranges.length > 0) CSS.highlights.set(NAME, new Highlight(...ranges));
  return ranges.length;
}

export function clearMatches() {
  if (supported()) CSS.highlights.delete(NAME);
}
