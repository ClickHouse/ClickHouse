import { readFile } from 'node:fs/promises';

const wasmAssetUrl = new URL('../../../docs/_site/customizations/clickhouse-sql-lexer-wasm.js', import.meta.url);
const highlighterSourceUrl = new URL('../../../docs/_site/customizations/clickhouse-sql-highlight.js', import.meta.url);

const TT = {
  Whitespace: 0,
  Comment: 1,
  BareWord: 2,
  Number: 3,
  StringLiteral: 4,
  QuotedIdentifier: 5,
  OpeningRoundBracket: 6,
  HereDoc: 17,
  DollarSign: 18,
  Plus: 19,
  Minus: 20,
  Slash: 21,
  Percent: 22,
  Arrow: 23,
  QuestionMark: 24,
  Colon: 25,
  Caret: 26,
  DoubleColon: 27,
  Equals: 28,
  NotEquals: 29,
  Less: 30,
  Greater: 31,
  LessOrEquals: 32,
  GreaterOrEquals: 33,
  Spaceship: 34,
  PipeMark: 35,
  Concatenation: 36,
  At: 37,
  DoubleAt: 38,
  Asterisk: 16,
};

const operatorTypes = new Set([
  TT.Asterisk,
  TT.Plus,
  TT.Minus,
  TT.Slash,
  TT.Percent,
  TT.Arrow,
  TT.QuestionMark,
  TT.Colon,
  TT.DoubleColon,
  TT.Caret,
  TT.Equals,
  TT.NotEquals,
  TT.Less,
  TT.Greater,
  TT.LessOrEquals,
  TT.GreaterOrEquals,
  TT.Spaceship,
  TT.PipeMark,
  TT.Concatenation,
  TT.At,
  TT.DoubleAt,
  TT.DollarSign,
]);

let lexerPromise;
let sqlKeywordsPromise;

function quotedStrings(source) {
  return [...source.matchAll(/'([^']+)'/g)].map((match) => match[1]);
}

async function sqlKeywords() {
  sqlKeywordsPromise ??= readFile(highlighterSourceUrl, 'utf8').then((source) => {
    const body = source.match(/var SQL_KEYWORDS = new Set\(\[([\s\S]*?)\]\);/)?.[1];
    if (!body) throw new Error('Could not read SQL keywords from the shared ClickHouse highlighter');
    return new Set(quotedStrings(body));
  });
  return sqlKeywordsPromise;
}

async function lexerExports() {
  lexerPromise ??= readFile(wasmAssetUrl, 'utf8').then(async (source) => {
    const assignment = source.split('window.__CH_SQL_LEXER_WASM_B64 =')[1];
    if (!assignment) throw new Error('Could not read the shared ClickHouse lexer payload');
    const fragments = [...assignment.matchAll(/"([A-Za-z0-9+/=]+)"/g)].map((match) => match[1]);
    if (fragments.length === 0) throw new Error('The shared ClickHouse lexer payload is empty');
    const bytes = Buffer.from(fragments.join(''), 'base64');
    return (await WebAssembly.instantiate(bytes)).instance.exports;
  });
  return lexerPromise;
}

function codeText(node) {
  if (node.type === 'text') return node.value;
  return Array.isArray(node.children) ? node.children.map(codeText).join('') : '';
}

function languageOf(code) {
  const classes = Array.isArray(code.properties.className)
    ? code.properties.className
    : String(code.properties.className ?? '').split(/\s+/);
  const languageClass = classes.find((name) => name.startsWith('language-'));
  return languageClass?.slice('language-'.length).toLowerCase() ?? '';
}

function tokenClass(tokens, index, keywords) {
  const token = tokens[index];
  switch (token.type) {
    case TT.Comment: return 'q-com';
    case TT.Number: return 'q-num';
    case TT.StringLiteral:
    case TT.HereDoc: return 'q-str';
    case TT.QuotedIdentifier: return 'q-qid';
    case TT.BareWord: {
      if (keywords.has(token.token.toUpperCase())) return 'q-kw';
      for (let next = index + 1; next < tokens.length; next += 1) {
        if (tokens[next].type === TT.Whitespace || tokens[next].type === TT.Comment) continue;
        return tokens[next].type === TT.OpeningRoundBracket ? 'q-fn' : 'q-id';
      }
      return 'q-id';
    }
    default: return operatorTypes.has(token.type) ? 'q-op' : '';
  }
}

function tokenizeSql(source, exports) {
  const bytes = new TextEncoder().encode(source);
  let offset = Number(exports.__heap_base?.value ?? 0);
  const lexer = offset;
  offset += exports.clickhouse_lexer_size;
  const queryBegin = offset;
  offset += bytes.length;
  const queryEnd = offset;
  const tokenBegin = offset;
  offset += 4;
  const tokenEnd = offset;
  offset += 4;

  if (offset > exports.memory.buffer.byteLength) {
    exports.memory.grow(Math.ceil((offset - exports.memory.buffer.byteLength) / 65536));
  }

  const buffer = exports.memory.buffer;
  new Uint8Array(buffer, queryBegin, bytes.length).set(bytes);
  exports.clickhouse_lexer_create(lexer, queryBegin, queryEnd, Math.max(65536, bytes.length));

  const decoder = new TextDecoder();
  const tokens = [];
  let previousEnd = queryBegin;
  while (true) {
    const type = exports.clickhouse_lexer_next_token(lexer, tokenBegin, tokenEnd);
    if (exports.clickhouse_lexer_token_is_error(type) || exports.clickhouse_lexer_token_is_end(type)) break;
    const view = new DataView(buffer);
    const begin = view.getUint32(tokenBegin, true);
    const end = view.getUint32(tokenEnd, true);
    if (end <= previousEnd) {
      throw new Error(`ClickHouse SQL lexer stopped advancing at byte ${end - queryBegin}`);
    }
    previousEnd = end;
    tokens.push({ type, token: decoder.decode(new Uint8Array(buffer, begin, end - begin)) });
  }
  return tokens;
}

function yamlSegments(line) {
  const segments = [];
  const pattern = /("(?:\\.|[^"\\])*"|'(?:''|[^'])*'|#[^\n]*|\b(?:true|false|null|yes|no|on|off)\b|[-+]?\b\d+(?:\.\d+)?\b|^[ \t-]*[A-Za-z0-9_.-]+(?=\s*:)|[&*][A-Za-z0-9_.-]+)/giy;
  let offset = 0;
  pattern.lastIndex = 0;
  while (offset < line.length) {
    pattern.lastIndex = offset;
    const match = pattern.exec(line);
    if (!match || match.index !== offset) {
      const next = line.slice(offset + 1).search(/["'#&*0-9A-Za-z+-]/);
      const end = next < 0 ? line.length : offset + 1 + next;
      segments.push({ text: line.slice(offset, end), className: '' });
      offset = end;
      continue;
    }
    const text = match[0];
    let className = '';
    if (text.trimStart().startsWith('#')) className = 'q-com';
    else if (/^['"]/.test(text)) className = 'q-str';
    else if (/^[&*]/.test(text)) className = 'q-fn';
    else if (/^(?:true|false|null|yes|no|on|off)$/i.test(text)) className = 'q-kw';
    else if (/[-+]?\d/.test(text)) className = 'q-num';
    else className = 'q-id';
    segments.push({ text, className });
    offset += text.length;
  }
  return segments;
}

function appendSegment(lines, text, className) {
  const parts = text.split('\n');
  for (let index = 0; index < parts.length; index += 1) {
    if (index > 0) lines.push([]);
    if (parts[index]) lines.at(-1).push({ text: parts[index], className });
  }
}

function lineNodes(lines) {
  return lines.flatMap((segments, index) => {
    const children = segments.map((segment) => segment.className
      ? {
          type: 'element',
          tagName: 'span',
          properties: { className: [segment.className] },
          children: [{ type: 'text', value: segment.text }],
        }
      : { type: 'text', value: segment.text });
    const line = {
      type: 'element',
      tagName: 'span',
      properties: { className: ['line'] },
      children,
    };
    return index === lines.length - 1 ? [line] : [line, { type: 'text', value: '\n' }];
  });
}

async function sqlLines(source) {
  const [exports, keywords] = await Promise.all([lexerExports(), sqlKeywords()]);
  const tokens = tokenizeSql(source, exports);
  const lines = [[]];
  let consumed = 0;
  for (let index = 0; index < tokens.length; index += 1) {
    const token = tokens[index];
    consumed += token.token.length;
    appendSegment(lines, token.token, tokenClass(tokens, index, keywords));
  }
  if (consumed < source.length) appendSegment(lines, source.slice(consumed), '');
  return lines;
}

export async function highlightCodeLines(language, source) {
  const normalizedLanguage = language.toLowerCase();
  if (normalizedLanguage === 'sql') return sqlLines(source);
  if (normalizedLanguage === 'yaml' || normalizedLanguage === 'yml') {
    return source.split('\n').map(yamlSegments);
  }
  return source.split('\n').map((line) => [{ text: line, className: '' }]);
}

async function highlightSql(code) {
  const lines = await highlightCodeLines('sql', codeText(code));
  code.children = lineNodes(lines);
  code.properties.className = ['language-sql', 'ch-code-hl', 'ch-sql-hl'];
}

async function highlightYaml(code) {
  code.children = lineNodes(await highlightCodeLines('yaml', codeText(code)));
  code.properties.className = ['language-yaml', 'ch-code-hl', 'ch-yaml-hl'];
}

async function visit(node) {
  if (!Array.isArray(node.children)) return;
  for (const child of node.children) {
    if (child.type === 'element' && child.tagName === 'pre') {
      const code = child.children.find((candidate) => candidate.type === 'element' && candidate.tagName === 'code');
      if (!code) continue;
      const language = languageOf(code);
      if (language === 'sql') await highlightSql(code);
      else if (language === 'yaml' || language === 'yml') await highlightYaml(code);
      continue;
    }
    await visit(child);
  }
}

export default function rehypeCustomCodeHighlighter() {
  return visit;
}
