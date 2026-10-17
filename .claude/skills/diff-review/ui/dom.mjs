// Tiny DOM helpers, so the modules that build UI do not each grow their own.

export const $ = (id) => document.getElementById(id);

/// `el('button', { class: 'hit', text: 'x', onClick: fn }, [child, …])`.
/// Unknown keys are assigned as properties (`title`, `type`, `hidden`, `value`…),
/// which is what every attribute this UI sets happens to want.
export function el(tag, props = {}, children = []) {
  const node = document.createElement(tag);
  for (const [key, value] of Object.entries(props)) {
    if (value == null) continue;
    if (key === 'class') node.className = value;
    else if (key === 'text') node.textContent = value;
    else if (key === 'style') node.style.cssText = value;
    else if (key.startsWith('on')) node.addEventListener(key.slice(2).toLowerCase(), value);
    else node[key] = value;
  }
  for (const child of children) if (child != null) node.append(child);
  return node;
}

/// Resolves after the browser has had a chance to paint. Rendering a file blocks
/// the main thread for as long as it takes to tokenize, so a spinner has to reach
/// the screen before that starts and can only come down a frame after it ends.
export function painted() {
  return new Promise((resolve) => requestAnimationFrame(() => requestAnimationFrame(resolve)));
}

/// Where the split handle sits: the boundary is a fraction of the *client* width
/// of the pane, so the handle does not drift by the scrollbar.
export function splitLeftPx(width, fraction) {
  return Math.round(width * fraction);
}

/// Work that may block for a few hundred milliseconds and must not land in the
/// middle of an interaction. Without `requestIdleCallback` a plain delay is the
/// closest thing to "after the page has settled".
export function whenIdle(fn) {
  const idle = window.requestIdleCallback ?? ((cb) => setTimeout(cb, 500));
  idle(fn);
}
