import { readdir, readFile, writeFile } from 'node:fs/promises';

import { highlightCodeLines } from './custom-code-highlighter.mjs';

const generatedComponentsUrl = new URL('../../.mintlify/components/', import.meta.url);
const codeBlockPattern = /<CodeBlock className="language-([^"]+)"><_components\.pre><_components\.code className="language-\1">\{("(?:\\.|[^"\\])*")\}<\/_components\.code><\/_components\.pre><\/CodeBlock>/g;

async function componentFiles(directory) {
  const entries = await readdir(directory, { withFileTypes: true });
  const nested = await Promise.all(entries.map((entry) => {
    const url = new URL(`${entry.name}${entry.isDirectory() ? '/' : ''}`, directory);
    if (entry.isDirectory()) return componentFiles(url);
    return entry.isFile() && entry.name.endsWith('.jsx') ? [url] : [];
  }));
  return nested.flat();
}

function jsxText(value) {
  return `{${JSON.stringify(value)}}`;
}

function jsxLines(lines) {
  return lines.map((segments, lineIndex) => {
    const children = segments.map((segment) => segment.className
      ? `<span className="${segment.className}">${jsxText(segment.text)}</span>`
      : jsxText(segment.text)).join('');
    const newline = lineIndex === lines.length - 1 ? '' : jsxText('\n');
    return `<span className="line">${children}</span>${newline}`;
  }).join('');
}

async function renderedCodeBlock(language, encodedSource) {
  const source = JSON.parse(encodedSource);
  const normalizedLanguage = language.toLowerCase();
  const highlighted = normalizedLanguage === 'sql' || normalizedLanguage === 'yaml' || normalizedLanguage === 'yml';
  const renderedLanguage = normalizedLanguage === 'yml' ? 'yaml' : normalizedLanguage;
  const codeClasses = [
    `language-${renderedLanguage}`,
    ...(highlighted ? ['ch-code-hl', renderedLanguage === 'sql' ? 'ch-sql-hl' : 'ch-yaml-hl'] : []),
  ].join(' ');
  const lines = await highlightCodeLines(normalizedLanguage, source);

  return `<div className="code-block" language="${renderedLanguage}" data-code-block=""><div className="code-block-floating-buttons"><button type="button" className="code-copy-button" data-code-copy="" data-testid="copy-code-button" aria-label="Copy the contents from the code block"><span className="code-copy-icon" aria-hidden="true"><svg className="code-copy-icon-copy" xmlns="http://www.w3.org/2000/svg" width="18" height="18" viewBox="0 0 18 18" fill="none" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round"><path d="M14.25 5.25H7.25C6.14543 5.25 5.25 6.14543 5.25 7.25V14.25C5.25 15.3546 6.14543 16.25 7.25 16.25H14.25C15.3546 16.25 16.25 15.3546 16.25 14.25V7.25C16.25 6.14543 15.3546 5.25 14.25 5.25Z"></path><path d="M2.80103 11.998L1.77203 5.07397C1.61003 3.98097 2.36403 2.96397 3.45603 2.80197L10.38 1.77297C11.313 1.63397 12.19 2.16297 12.528 3.00097"></path></svg><svg className="code-copy-icon-copied" xmlns="http://www.w3.org/2000/svg" width="18" height="18" viewBox="0 0 18 18" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round"><path d="M2.75 9.5L6.5 13.25L15.25 4.5"></path></svg></span><span className="code-copy-tooltip" aria-hidden="true">Copy</span></button></div><div className="code-block-background" data-component-part="code-block-root" tabIndex={0}><pre className="code-block-pre"><code className="${codeClasses}">${jsxLines(lines)}</code></pre></div></div>`;
}

async function rewriteFile(file) {
  const source = await readFile(file, 'utf8');
  const matches = [...source.matchAll(codeBlockPattern)];
  if (matches.length === 0) return 0;

  let rewritten = '';
  let offset = 0;
  for (const match of matches) {
    rewritten += source.slice(offset, match.index);
    rewritten += await renderedCodeBlock(match[1], match[2]);
    offset = match.index + match[0].length;
  }
  rewritten += source.slice(offset);
  if (rewritten.includes('<CodeBlock')) {
    throw new Error(`Could not replace every generated CodeBlock in ${file.pathname}`);
  }
  await writeFile(file, rewritten);
  return matches.length;
}

export default function rewriteGeneratedCodeBlocks() {
  return {
    name: 'clickhouse-reference-code-blocks',
    hooks: {
      'astro:config:setup': async ({ logger }) => {
        const files = await componentFiles(generatedComponentsUrl);
        const counts = await Promise.all(files.map(rewriteFile));
        const count = counts.reduce((sum, value) => sum + value, 0);
        logger.info(`Rendered ${count} compound code blocks without Shiki`);
      },
    },
  };
}
