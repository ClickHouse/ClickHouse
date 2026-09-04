import { readFile, readdir } from 'node:fs/promises';
import path from 'node:path';

function skipTrivia(source, start) {
  let index = start;
  while (index < source.length) {
    if (/\s/.test(source[index])) {
      index += 1;
      continue;
    }
    if (source.startsWith('//', index)) {
      const newline = source.indexOf('\n', index + 2);
      return newline === -1 ? source.length : skipTrivia(source, newline + 1);
    }
    if (source.startsWith('/*', index)) {
      const end = source.indexOf('*/', index + 2);
      if (end === -1) throw new Error('Unterminated C++ block comment');
      index = end + 2;
      continue;
    }
    break;
  }
  return index;
}

function parseCppString(source, start, sourcePath) {
  const index = skipTrivia(source, start);
  if (source.startsWith('R"', index)) {
    const delimiterEnd = source.indexOf('(', index + 2);
    if (delimiterEnd === -1) throw new Error(`Invalid raw string in ${sourcePath}`);
    const delimiter = source.slice(index + 2, delimiterEnd);
    if (delimiter.length > 16 || /[\s\\()]/.test(delimiter)) {
      throw new Error(`Invalid raw string delimiter in ${sourcePath}`);
    }
    const closing = `)${delimiter}"`;
    const valueEnd = source.indexOf(closing, delimiterEnd + 1);
    if (valueEnd === -1) throw new Error(`Unterminated raw string in ${sourcePath}`);
    return {
      value: source.slice(delimiterEnd + 1, valueEnd).trim(),
      end: valueEnd + closing.length,
    };
  }

  if (source[index] !== '"') {
    throw new Error(`Expected a C++ string literal in ${sourcePath}`);
  }
  let end = index + 1;
  while (end < source.length) {
    if (source[end] === '\\') {
      end += 2;
      continue;
    }
    if (source[end] === '"') break;
    end += 1;
  }
  if (end >= source.length) throw new Error(`Unterminated C++ string in ${sourcePath}`);
  const literal = source.slice(index, end + 1);
  try {
    return { value: JSON.parse(literal), end: end + 1 };
  } catch (error) {
    throw new Error(`Cannot decode a C++ string in ${sourcePath}: ${error.message}`);
  }
}

function skipCppToken(source, index, sourcePath) {
  if (source.startsWith('R"', index) || source[index] === '"') {
    return parseCppString(source, index, sourcePath).end;
  }
  if (source[index] === "'") {
    let end = index + 1;
    while (end < source.length) {
      if (source[end] === '\\') end += 2;
      else if (source[end] === "'") return end + 1;
      else end += 1;
    }
    throw new Error(`Unterminated C++ character literal in ${sourcePath}`);
  }
  if (source.startsWith('//', index)) {
    const newline = source.indexOf('\n', index + 2);
    return newline === -1 ? source.length : newline + 1;
  }
  if (source.startsWith('/*', index)) {
    const end = source.indexOf('*/', index + 2);
    if (end === -1) throw new Error(`Unterminated C++ block comment in ${sourcePath}`);
    return end + 2;
  }
  return index + 1;
}

function findFieldEnd(source, start, sourcePath) {
  const stack = [];
  const pairs = { '(': ')', '[': ']', '{': '}' };
  for (let index = start; index < source.length;) {
    if (
      source.startsWith('R"', index)
      || source[index] === '"'
      || source[index] === "'"
      || source.startsWith('//', index)
      || source.startsWith('/*', index)
    ) {
      index = skipCppToken(source, index, sourcePath);
      continue;
    }
    const character = source[index];
    if (pairs[character]) stack.push(pairs[character]);
    else if (stack.at(-1) === character) stack.pop();
    else if (stack.length === 0 && (character === ',' || character === '}')) {
      return { end: index, delimiter: character };
    }
    index += 1;
  }
  throw new Error(`Unterminated documentation field in ${sourcePath}`);
}

function parseInitializer(source, start, sourcePath) {
  const fields = new Map();
  let index = start + 1;
  while (index < source.length) {
    index = skipTrivia(source, index);
    if (source[index] === '}') return { fields, end: index + 1 };
    if (source[index] !== '.') throw new Error(`Expected a designated field in ${sourcePath}`);
    const nameMatch = source.slice(index + 1).match(/^([A-Za-z_]\w*)/);
    if (!nameMatch) throw new Error(`Invalid documentation field in ${sourcePath}`);
    const name = nameMatch[1];
    index = skipTrivia(source, index + 1 + name.length);
    if (source[index] !== '=') throw new Error(`Expected = after .${name} in ${sourcePath}`);
    const valueStart = skipTrivia(source, index + 1);
    const { end, delimiter } = findFieldEnd(source, valueStart, sourcePath);
    if (fields.has(name)) throw new Error(`Duplicate .${name} field in ${sourcePath}`);
    fields.set(name, source.slice(valueStart, end).trim());
    index = delimiter === ',' ? end + 1 : end;
  }
  throw new Error(`Unterminated documentation initializer in ${sourcePath}`);
}

function parseStringExpression(expression, sourcePath, field) {
  const parsed = parseCppString(expression, 0, sourcePath);
  if (skipTrivia(expression, parsed.end) !== expression.length) {
    throw new Error(`Unsupported .${field} expression in ${sourcePath}`);
  }
  return parsed.value;
}

function parseStringList(expression, sourcePath, field) {
  let index = skipTrivia(expression, 0);
  if (expression[index] !== '{') throw new Error(`Expected a list for .${field} in ${sourcePath}`);
  index += 1;
  const values = [];
  while (index < expression.length) {
    index = skipTrivia(expression, index);
    if (expression[index] === '}') {
      if (skipTrivia(expression, index + 1) !== expression.length) {
        throw new Error(`Unexpected content after .${field} in ${sourcePath}`);
      }
      return values;
    }
    const parsed = parseCppString(expression, index, sourcePath);
    values.push(parsed.value);
    index = skipTrivia(expression, parsed.end);
    if (expression[index] === ',') index += 1;
    else if (expression[index] !== '}') throw new Error(`Invalid .${field} list in ${sourcePath}`);
  }
  throw new Error(`Unterminated .${field} list in ${sourcePath}`);
}

function normalizedSectionLabel(line) {
  let value = line.trim();
  value = value.replace(/^#{1,6}\s+/, '').replace(/\s+\{#[^}]+\}\s*$/, '').trim();
  value = value.replace(/^\*\*(.+)\*\*$/, '$1').replace(/:$/, '').trim();
  return value.toLocaleLowerCase();
}

function markdownLinesOutsideCode(content) {
  const lines = [];
  let inCodeBlock = false;
  for (const line of content.split('\n')) {
    if (/^\s*```/.test(line)) {
      inCodeBlock = !inCodeBlock;
      continue;
    }
    if (!inCodeBlock) lines.push(line);
  }
  return lines;
}

function isFullPageDescription(description) {
  return markdownLinesOutsideCode(description).some((line) => (
    /^#\s+/.test(line) || /^#{2,6}\s+.+\s+\{#[^}]+\}\s*$/.test(line)
  ));
}

function descriptionDocumentsSyntax(description) {
  const lines = markdownLinesOutsideCode(description);
  return lines.some((line) => normalizedSectionLabel(line) === 'syntax')
    || description.split('\n').some((line, index, allLines) => (
      /syntax:\s*$/i.test(line.trim()) && /^\s*```sql\s*$/i.test(allLines[index + 1] ?? '')
    ));
}

function descriptionDocumentsRelated(description) {
  return markdownLinesOutsideCode(description).some((line) => (
    ['related', 'related content', 'related statements', 'see also']
      .includes(normalizedSectionLabel(line))
  ));
}

function composeMarkdown({ description, syntax, parent, related }) {
  if (isFullPageDescription(description)) return description.trim();
  let result = description.trim();
  const append = (block) => {
    result = [result, block].filter(Boolean).join('\n\n');
  };
  const normalizedSyntax = syntax.trim();
  if (
    normalizedSyntax
    && !descriptionDocumentsSyntax(result)
    && !result.includes(normalizedSyntax)
  ) {
    append(`**Syntax**\n\n\`\`\`sql\n${normalizedSyntax}\n\`\`\``);
  }
  if (parent.trim()) append(`**Part of:** \`${parent.trim()}\``);
  if (related.length > 0 && !descriptionDocumentsRelated(result)) {
    append(`**Related:** ${related.map((name) => `\`${name}\``).join(', ')}`);
  }
  return result;
}

function parseStatementRegistrations(source, sourcePath) {
  const registrations = [];
  const marker = 'factory.registerStatement';
  let cursor = 0;
  while (cursor < source.length) {
    const markerIndex = source.indexOf(marker, cursor);
    if (markerIndex === -1) break;
    let index = skipTrivia(source, markerIndex + marker.length);
    if (source[index] !== '(') throw new Error(`Expected registerStatement call in ${sourcePath}`);
    index = skipTrivia(source, index + 1);
    const name = parseCppString(source, index, sourcePath);
    index = skipTrivia(source, name.end);
    if (source[index] !== ',') throw new Error(`Expected statement documentation in ${sourcePath}`);
    index = skipTrivia(source, index + 1);
    if (source[index] !== '{') throw new Error(`Expected statement initializer in ${sourcePath}`);
    const { fields, end } = parseInitializer(source, index, sourcePath);
    for (const required of ['description', 'syntax', 'related']) {
      if (!fields.has(required)) throw new Error(`Statement ${name.value} has no .${required} in ${sourcePath}`);
    }
    for (const field of fields.keys()) {
      if (!['description', 'syntax', 'parent', 'related'].includes(field)) {
        throw new Error(`Statement ${name.value} uses unsupported .${field} in ${sourcePath}`);
      }
    }
    const documentation = {
      description: parseStringExpression(fields.get('description'), sourcePath, 'description'),
      syntax: parseStringExpression(fields.get('syntax'), sourcePath, 'syntax'),
      parent: fields.has('parent')
        ? parseStringExpression(fields.get('parent'), sourcePath, 'parent')
        : '',
      related: parseStringList(fields.get('related'), sourcePath, 'related'),
    };
    registrations.push({
      name: name.value,
      content: composeMarkdown(documentation),
      sourcePath,
      parent: documentation.parent,
      related: documentation.related,
    });
    cursor = end;
  }
  return registrations;
}

async function collectCppFiles(directory) {
  const entries = await readdir(directory, { withFileTypes: true });
  const files = [];
  for (const entry of entries.sort((left, right) => left.name.localeCompare(right.name))) {
    const entryPath = path.join(directory, entry.name);
    if (entry.isDirectory()) files.push(...(await collectCppFiles(entryPath)));
    else if (entry.name.endsWith('.cpp')) files.push(entryPath);
  }
  return files;
}

export async function loadStatementRegistrations(sourceRoot) {
  const parserDirectory = path.join(sourceRoot, 'src/Parsers');
  const registrations = [];
  for (const sourceFile of await collectCppFiles(parserDirectory)) {
    const sourcePath = path.relative(sourceRoot, sourceFile).split(path.sep).join('/');
    const source = await readFile(sourceFile, 'utf8');
    registrations.push(...parseStatementRegistrations(source, sourcePath));
  }
  const names = new Set();
  for (const registration of registrations) {
    if (names.has(registration.name)) {
      throw new Error(`Duplicate statement registration: ${registration.name}`);
    }
    names.add(registration.name);
  }
  return registrations.sort((left, right) => left.name.localeCompare(right.name));
}

export { composeMarkdown, parseStatementRegistrations };
