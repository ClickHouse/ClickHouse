// ClickHouse-native SQL syntax highlighting for Mintlify.
//
// Port of clickhouse-docs' WASM highlighter (PR ClickHouse/clickhouse-docs#6308)
// to a Mintlify custom script. Upstream swizzled Docusaurus's code-block
// renderer; Mintlify has no equivalent, so instead we enhance the already
// rendered DOM:
//
//   1. Mintlify highlights code blocks at build time with Shiki, emitting
//      `<div class="code-block" language="sql"> ... <pre class="shiki"><code>
//      <span class="line"><span style="color:...">token</span>...</span></code></pre>`.
//   2. For blocks tagged `language="sql"` we reconstruct the source text, run it
//      through ClickHouse's own SQL lexer (compiled to WASM, the same method and
//      color scheme as `programs/server/play.html`), and replace Shiki's token
//      spans with class-based segments (`q-kw`, `q-id`, `q-fn`, ...). Because our
//      spans carry no inline `style`, the palette CSS below wins cleanly.
//
// Every other language is left exactly as Shiki rendered it. Re-applied on SPA
// navigation via a MutationObserver, matching the other scripts in this folder.
/* eslint-disable */
(function () {
  'use strict';

  // Numeric TokenType values, matching the order of the C++ enum in
  // src/Parsers/Lexer.h. Only the categories we classify are named.
  var TT = {
    Whitespace: 0, Comment: 1, BareWord: 2, Number: 3, StringLiteral: 4,
    QuotedIdentifier: 5, OpeningRoundBracket: 6, HereDoc: 17, DollarSign: 18,
    Plus: 19, Minus: 20, Slash: 21, Percent: 22, Arrow: 23, QuestionMark: 24,
    Colon: 25, Caret: 26, DoubleColon: 27, Equals: 28, NotEquals: 29, Less: 30,
    Greater: 31, LessOrEquals: 32, GreaterOrEquals: 33, Spaceship: 34,
    PipeMark: 35, Concatenation: 36, At: 37, DoubleAt: 38, Asterisk: 16,
  };

  // SQL keywords recognized for highlighting. The lexer reports them as
  // BareWord, so we disambiguate identifiers from keywords here. Comparisons are
  // case-insensitive. Kept in sync with play.html / upstream highlighter.ts.
  var SQL_KEYWORDS = new Set([
    'ADD', 'AFTER', 'ALL', 'ALTER', 'AND', 'ANTI', 'ANY', 'ARRAY', 'AS', 'ASC', 'ASCENDING',
    'ASOF', 'AST', 'ASYNC', 'ATTACH', 'BACKUP', 'BEGIN', 'BETWEEN', 'BOTH', 'BY',
    'CACHE', 'CASCADE', 'CASE', 'CAST', 'CHANGE', 'CHANGED', 'CHECK', 'CLEAR', 'CLUSTER',
    'CODEC', 'COLLATE', 'COLUMN', 'COLUMNS', 'COMMENT', 'COMMIT', 'CONSTRAINT', 'CREATE',
    'CROSS', 'CUBE', 'CURRENT',
    'DATABASE', 'DATABASES', 'DAY', 'DEDUPLICATE', 'DEFAULT', 'DELETE', 'DESC', 'DESCENDING',
    'DESCRIBE', 'DETACH', 'DICTIONARIES', 'DICTIONARY', 'DISK', 'DISTINCT', 'DISTRIBUTED',
    'DROP', 'ELSE', 'END', 'ENGINE', 'ESTIMATE', 'EVENTS', 'EXCEPT', 'EXCHANGE', 'EXISTS',
    'EXPLAIN', 'EXPRESSION', 'EXTENDED', 'EXTRACT',
    'FALSE', 'FETCH', 'FETCHES', 'FILE', 'FILESYSTEM', 'FINAL', 'FIRST', 'FLUSH', 'FOLLOWING',
    'FOR', 'FOREIGN', 'FORMAT', 'FREEZE', 'FROM', 'FULL', 'FUNCTION',
    'GLOBAL', 'GRANT', 'GROUP', 'GROUPS', 'HAVING', 'HIERARCHICAL', 'HOUR',
    'ID', 'IDENTIFIED', 'IF', 'ILIKE', 'IN', 'INDEX', 'INF', 'INHERIT', 'INJECTIVE',
    'INNER', 'INSERT', 'INTERSECT', 'INTERVAL', 'INTO', 'INVISIBLE', 'IS', 'IS_OBJECT_ID',
    'JOIN', 'KEY', 'KEYED', 'KILL',
    'LAST', 'LATERAL', 'LAYOUT', 'LEADING', 'LEFT', 'LIFETIME', 'LIKE', 'LIMIT', 'LIMITS',
    'LIVE', 'LOCAL', 'LOGS',
    'MATERIALIZE', 'MATERIALIZED', 'MAX', 'MERGES', 'MICROSECOND', 'MILLISECOND', 'MIN',
    'MINUTE', 'MODIFY', 'MONTH', 'MOVE', 'MUTATION',
    'NAN_SQL', 'NEXT', 'NO', 'NONE', 'NOT', 'NULL', 'NULLS',
    'OFFSET', 'ON', 'ONLY', 'OPTIMIZE', 'OPTION', 'OR', 'ORDER', 'OUTER', 'OUTFILE', 'OVER',
    'PARTITION', 'PASTE', 'PERMANENTLY', 'PLAN', 'POPULATE', 'PRECEDING', 'PRECISION',
    'PREWHERE', 'PRIMARY', 'PROFILE', 'PROJECTION', 'QUARTER', 'QUERY', 'QUOTA',
    'RANDOMIZED', 'RANGE', 'RECURSIVE', 'REFRESH', 'REGEXP', 'RELOAD', 'REMOTE', 'RENAME',
    'REPLACE', 'REPLICA', 'REPLICAS', 'RESET', 'RESTORE', 'RESTRICT', 'RESTRICTIVE',
    'RETURNS', 'REVOKE', 'RIGHT', 'ROLE', 'ROLLBACK', 'ROLLUP', 'ROW', 'ROWS',
    'SAMPLE', 'SECOND', 'SELECT', 'SEMI', 'SENDS', 'SET', 'SETS', 'SETTINGS', 'SHARD',
    'SHOW', 'SIGNED', 'SOURCE', 'SQL_SECURITY', 'START', 'STEP', 'STORAGE', 'STRICT',
    'STRICTLY_ASCENDING', 'SUBPARTITION', 'SUBSTRING', 'SUSPEND', 'SYNC', 'SYNTAX', 'SYSTEM',
    'TABLE', 'TABLES', 'TEMPORARY', 'TEST', 'THEN', 'TIES', 'TIMESTAMP', 'TO', 'TOP',
    'TOTALS', 'TRACKING', 'TRAILING', 'TRANSACTION', 'TRIGGER', 'TRIM', 'TRUE', 'TRUNCATE',
    'TYPE',
    'UNBOUNDED', 'UNFREEZE', 'UNION', 'UNIQUE', 'UNSIGNED', 'UPDATE', 'USE', 'USING',
    'UUID', 'VALUES', 'VARYING', 'VIEW', 'VIRTUAL', 'VISIBLE',
    'WATCH', 'WEEK', 'WHEN', 'WHERE', 'WINDOW', 'WITH', 'WORK', 'WRITABLE',
    'XOR', 'YEAR', 'ZKPATH',
  ]);

  var OPERATOR_TYPES = new Set([
    TT.Asterisk, TT.Plus, TT.Minus, TT.Slash, TT.Percent, TT.Arrow, TT.QuestionMark,
    TT.Colon, TT.DoubleColon, TT.Caret, TT.Equals, TT.NotEquals, TT.Less, TT.Greater,
    TT.LessOrEquals, TT.GreaterOrEquals, TT.Spaceship, TT.PipeMark, TT.Concatenation,
    TT.At, TT.DoubleAt, TT.DollarSign,
  ]);

  var lexerPromise = null;

  // The WASM payload lives in a separate script (clickhouse-sql-lexer-wasm.js).
  // Mintlify loads custom scripts asynchronously, so the order in docs.json is
  // not guaranteed — wait (briefly) for the global rather than failing if this
  // script happens to run first.
  function waitForWasmGlobal() {
    if (typeof window.__CH_SQL_LEXER_WASM_B64 === 'string') {
      return Promise.resolve(window.__CH_SQL_LEXER_WASM_B64);
    }
    return new Promise(function (resolve, reject) {
      var waited = 0;
      var iv = setInterval(function () {
        if (typeof window.__CH_SQL_LEXER_WASM_B64 === 'string') {
          clearInterval(iv);
          resolve(window.__CH_SQL_LEXER_WASM_B64);
        } else if ((waited += 50) >= 10000) {
          clearInterval(iv);
          reject(new Error('ClickHouse SQL lexer WASM not loaded'));
        }
      }, 50);
    });
  }

  // Instantiate the embedded Lexer.wasm exactly once and cache the exports.
  function loadLexer() {
    if (!lexerPromise) {
      lexerPromise = waitForWasmGlobal().then(function (b64) {
        var binary = atob(b64);
        var bytes = new Uint8Array(binary.length);
        for (var i = 0; i < binary.length; i++) bytes[i] = binary.charCodeAt(i);
        return WebAssembly.instantiate(bytes).then(function (module) {
          return module.instance.exports;
        });
      }).catch(function (e) {
        lexerPromise = null; // allow a later block to retry
        throw e;
      });
    }
    return lexerPromise;
  }

  // Tokenize a SQL string using the ClickHouse lexer compiled to WASM.
  function tokenize(query) {
    return loadLexer().then(function (exports) {
      var queryBytes = new TextEncoder().encode(query);

      var required = exports.clickhouse_lexer_size + queryBytes.length + 8;
      if (exports.memory.buffer.byteLength < required) {
        var pages = Math.ceil((required - exports.memory.buffer.byteLength) / 65536);
        exports.memory.grow(pages);
      }

      var buffer = exports.memory.buffer;
      var memoryOffset = 0;

      var lexer = memoryOffset;
      memoryOffset += exports.clickhouse_lexer_size;

      var queryArray = new Uint8Array(buffer, memoryOffset, queryBytes.length);
      queryArray.set(queryBytes);
      var queryBegin = memoryOffset;
      memoryOffset += queryBytes.length;
      var queryEnd = memoryOffset;

      exports.clickhouse_lexer_create(lexer, queryBegin, queryEnd, Math.max(65536, queryBytes.length));

      var tokenBegin = memoryOffset;
      memoryOffset += 4;
      var tokenEnd = memoryOffset;
      memoryOffset += 4;

      var view = new DataView(buffer);
      var decoder = new TextDecoder();
      var result = [];

      while (true) {
        var tokenType = exports.clickhouse_lexer_next_token(lexer, tokenBegin, tokenEnd);
        if (
          exports.clickhouse_lexer_token_is_error(tokenType) ||
          exports.clickhouse_lexer_token_is_end(tokenType)
        ) {
          break;
        }
        var begin = view.getUint32(tokenBegin, true);
        var end = view.getUint32(tokenEnd, true);
        var token = decoder.decode(new Uint8Array(buffer, begin, end - begin));
        result.push({ type: tokenType, token: token });
      }
      return result;
    });
  }

  // Map a single token to a CSS class. For BareWords we peek at the next
  // non-whitespace token to distinguish a function call (`foo(`) from a plain
  // identifier — the lexer alone cannot tell them apart.
  function tokenClass(tokens, i) {
    var elem = tokens[i];
    switch (elem.type) {
      case TT.Comment: return 'q-com';
      case TT.Number: return 'q-num';
      case TT.StringLiteral:
      case TT.HereDoc: return 'q-str';
      case TT.QuotedIdentifier: return 'q-qid';
      case TT.BareWord: {
        if (SQL_KEYWORDS.has(elem.token.toUpperCase())) return 'q-kw';
        for (var j = i + 1; j < tokens.length; ++j) {
          if (tokens[j].type === TT.Whitespace) continue;
          return tokens[j].type === TT.OpeningRoundBracket ? 'q-fn' : 'q-id';
        }
        return 'q-id';
      }
      default:
        return OPERATOR_TYPES.has(elem.type) ? 'q-op' : '';
    }
  }

  // Append `text` (which may contain newlines) to `lines`, starting a new line
  // for each `\n`.
  function pushSegment(lines, text, className) {
    var parts = text.split('\n');
    for (var j = 0; j < parts.length; j++) {
      if (j > 0) lines.push([]);
      if (parts[j].length > 0) {
        lines[lines.length - 1].push({ text: parts[j], className: className });
      }
    }
  }

  // Tokenize `code` and return it as an array of lines, where each line is an
  // array of styled segments.
  function buildHighlightedLines(code) {
    return tokenize(code).then(function (tokens) {
      var lines = [[]];
      var consumed = 0;
      for (var i = 0; i < tokens.length; i++) {
        var text = tokens[i].token;
        consumed += text.length;
        pushSegment(lines, text, tokenClass(tokens, i));
      }
      // Any tail not covered by tokens (lexer error / size limit) is rendered
      // plain rather than as an error: docs SQL blocks may be followed by
      // non-SQL text. Plain output keeps the block readable.
      if (consumed < code.length) {
        pushSegment(lines, code.slice(consumed), '');
      }
      return lines;
    });
  }

  // ---- DOM integration ------------------------------------------------------

  var STYLE_ID = 'ch-sql-highlight-styles';

  // The exact light + dark palette from play.html / clickhouse-client, adapted
  // to Mintlify's dark-mode carrier (`<html class="dark">`). Scoped to
  // `.ch-sql-hl` so only blocks we rebuilt are affected. `!important` is
  // required because Mintlify forces `html.dark .shiki span { color:
  // var(--shiki-dark) !important }` on every token; our higher-specificity
  // `.dark .ch-sql-hl .q-*` rules only win when they are also `!important`.
  function injectStyles() {
    if (document.getElementById(STYLE_ID)) return;
    var css =
      '.ch-sql-hl .q-kw{font-weight:bold !important}' +
      '.ch-sql-hl .q-com{font-style:italic !important;color:#757575 !important}' +
      '.ch-sql-hl .q-id{color:#00838f !important}' +
      '.ch-sql-hl .q-fn{color:#875f00 !important}' +
      '.ch-sql-hl .q-num{color:#008700 !important}' +
      '.ch-sql-hl .q-str{color:#006400 !important}' +
      '.ch-sql-hl .q-qid{color:#008b8b !important}' +
      '.dark .ch-sql-hl .q-id{color:#00cdcd !important}' +
      '.dark .ch-sql-hl .q-fn{color:#cdcd00 !important}' +
      '.dark .ch-sql-hl .q-num{color:#00d700 !important}' +
      '.dark .ch-sql-hl .q-str{color:#00cd00 !important}' +
      '.dark .ch-sql-hl .q-qid{color:#00d7d7 !important}' +
      '.dark .ch-sql-hl .q-com{color:#9e9e9e !important}';
    var style = document.createElement('style');
    style.id = STYLE_ID;
    style.textContent = css;
    document.head.appendChild(style);
  }

  // Replace a line element's Shiki token spans with our class-based segments.
  // Uses textContent (not innerHTML) so SQL characters like `<` and `&` are
  // never interpreted as markup.
  function renderLine(lineEl, segments) {
    while (lineEl.firstChild) lineEl.removeChild(lineEl.firstChild);
    if (segments.length === 0) return;
    for (var i = 0; i < segments.length; i++) {
      var seg = segments[i];
      if (seg.className) {
        var span = document.createElement('span');
        span.className = seg.className;
        span.textContent = seg.text;
        lineEl.appendChild(span);
      } else {
        lineEl.appendChild(document.createTextNode(seg.text));
      }
    }
  }

  function enhanceBlock(block) {
    var code = block.querySelector('pre.shiki code');
    if (!code) return;
    if (code.dataset.chSqlState) return; // already processed / in flight / failed

    var lineEls = code.querySelectorAll(':scope > span.line');
    if (lineEls.length === 0) return;

    var source = Array.prototype.map.call(lineEls, function (l) {
      return l.textContent;
    }).join('\n');

    code.dataset.chSqlState = 'pending';

    buildHighlightedLines(source).then(function (lines) {
      // Only rebuild when the line structure matches exactly; otherwise leave
      // Shiki's output untouched to avoid corrupting the block.
      if (lines.length !== lineEls.length) {
        code.dataset.chSqlState = 'skipped';
        return;
      }
      injectStyles();
      for (var i = 0; i < lineEls.length; i++) renderLine(lineEls[i], lines[i]);
      code.classList.add('ch-sql-hl');
      code.dataset.chSqlState = 'done';
    }).catch(function (e) {
      code.dataset.chSqlState = 'failed';
      // eslint-disable-next-line no-console
      console.error('ClickHouse SQL highlighting failed:', e);
    });
  }

  function scan() {
    var blocks = document.querySelectorAll('.code-block[language="sql"]');
    for (var i = 0; i < blocks.length; i++) enhanceBlock(blocks[i]);
  }

  function init() {
    scan();
    // Re-scan on SPA navigation / late renders. Our own edits set chSqlState so
    // already-handled blocks are skipped, preventing an observer feedback loop.
    var scheduled = false;
    var observer = new MutationObserver(function () {
      if (scheduled) return;
      scheduled = true;
      requestAnimationFrame(function () { scheduled = false; scan(); });
    });
    observer.observe(document.documentElement, { childList: true, subtree: true });
  }

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', init);
  } else {
    init();
  }
})();