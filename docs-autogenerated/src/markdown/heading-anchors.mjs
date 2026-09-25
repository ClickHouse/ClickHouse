import Slugger from 'github-slugger';

function element(tagName, properties = {}, children = []) {
  return { type: 'element', tagName, properties, children };
}

function textContent(node) {
  if (node.type === 'text') return node.value;
  return Array.isArray(node.children) ? node.children.map(textContent).join('') : '';
}

function headingAnchor(id, label) {
  return element('a', {
    className: ['heading-anchor'],
    href: `#${id}`,
    ariaLabel: `Link to ${label}`,
  }, [
    element('svg', {
      viewBox: '0 0 24 24',
      fill: 'none',
      stroke: 'currentColor',
      strokeWidth: '2',
      strokeLinecap: 'round',
      strokeLinejoin: 'round',
      ariaHidden: 'true',
    }, [
      element('path', { d: 'M10 13a5 5 0 0 0 7.54.54l3-3a5 5 0 0 0-7.07-7.07l-1.72 1.71' }),
      element('path', { d: 'M14 11a5 5 0 0 0-7.54-.54l-3 3a5 5 0 0 0 7.07 7.07l1.71-1.71' }),
    ]),
  ]);
}

function visit(node, slugger) {
  if (!Array.isArray(node.children)) return;
  for (const child of node.children) {
    if (
      child.type === 'element'
      && /^h[1-6]$/.test(child.tagName)
    ) {
      const label = textContent(child).trim();
      child.properties ??= {};
      if (typeof child.properties.id !== 'string') child.properties.id = slugger.slug(label);
      if (/^h[2-6]$/.test(child.tagName)) {
        child.children.unshift(headingAnchor(child.properties.id, label));
      }
    }
    visit(child, slugger);
  }
}

export default function rehypeHeadingAnchors() {
  return (tree) => visit(tree, new Slugger());
}
