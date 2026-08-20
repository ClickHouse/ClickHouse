function element(tagName, properties = {}, children = []) {
  return { type: 'element', tagName, properties, children };
}

function classes(properties) {
  const value = properties.className ?? properties.class;
  if (Array.isArray(value)) return value.map(String);
  if (typeof value === 'string') return value.split(/\s+/).filter(Boolean);
  return [];
}

function codeLanguage(pre, code) {
  const fromPre = pre.properties.dataLanguage;
  if (typeof fromPre === 'string' && fromPre) return fromPre.toLowerCase();

  const languageClass = classes(code.properties).find((name) => name.startsWith('language-'));
  return languageClass?.slice('language-'.length).toLowerCase() || 'text';
}

function codeMeta(pre, code) {
  for (const value of [pre.properties.dataMeta, code.data?.meta, code.properties.metastring]) {
    if (typeof value === 'string' && value) return value;
  }
  return '';
}

function codeTitle(meta) {
  const match = meta.match(/(?:^|\s)(?:title|filename)=(?:"([^"]*)"|'([^']*)'|([^\s]+))/i);
  return match?.[1] || match?.[2] || match?.[3] || '';
}

function copyButton() {
  return element('button', {
    type: 'button',
    className: ['code-copy-button'],
    'data-code-copy': '',
    'data-testid': 'copy-code-button',
    ariaLabel: 'Copy the contents from the code block',
  }, [
    element('span', { className: ['code-copy-icon'], ariaHidden: 'true' }),
    element('span', { className: ['code-copy-tooltip'], ariaHidden: 'true' }, [
      { type: 'text', value: 'Copy' },
    ]),
  ]);
}

function wrapCodeBlock(pre) {
  const code = pre.children.find((child) => child.type === 'element' && child.tagName === 'code');
  if (!code) return pre;

  const language = codeLanguage(pre, code);
  const title = codeTitle(codeMeta(pre, code));
  pre.properties.className = [...new Set([...classes(pre.properties), 'code-block-pre'])];
  delete pre.properties.class;
  delete pre.properties.tabIndex;
  delete pre.properties.tabindex;

  const actions = title
    ? element('div', { className: ['code-block-actions'] }, [copyButton()])
    : element('div', { className: ['code-block-floating-buttons'] }, [copyButton()]);
  const body = element('div', {
    className: ['code-block-background'],
    'data-component-part': 'code-block-root',
    tabIndex: 0,
  }, [pre]);
  const children = title
    ? [
        element('div', {
          className: ['code-block-header'],
          'data-component-part': 'code-block-header',
        }, [
          element('div', {
            className: ['code-block-filename'],
            'data-component-part': 'code-block-header-filename',
          }, [{ type: 'text', value: title }]),
          actions,
        ]),
        body,
      ]
    : [actions, body];

  return element('div', {
    className: ['code-block', ...(title ? ['code-block-with-header'] : [])],
    language,
    'data-code-block': '',
  }, children);
}

function visit(node) {
  if (!Array.isArray(node.children)) return;
  for (let index = 0; index < node.children.length; index += 1) {
    const child = node.children[index];
    if (child.type === 'element' && child.tagName === 'pre') {
      node.children[index] = wrapCodeBlock(child);
      continue;
    }
    visit(child);
  }
}

export default function rehypeCodeBlocks() {
  return visit;
}
