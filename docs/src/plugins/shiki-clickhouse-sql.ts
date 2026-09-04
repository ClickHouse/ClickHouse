import {
  TokenType,
  isSignificant,
  loadLexer,
  tokenizeSync,
} from "@clickhouse/lexer";
import keywords from "../generated/clickhouse-sql-keywords.json";

// Shiki transformers are synchronous. Instantiate the package's embedded WASM
// while Astro loads its configuration so every code block can tokenize sync.
await loadLexer();

const sqlKeywords = new Set(keywords);
const operatorTypes = new Set([
  TokenType.Asterisk,
  TokenType.Plus,
  TokenType.Minus,
  TokenType.Slash,
  TokenType.Percent,
  TokenType.Arrow,
  TokenType.QuestionMark,
  TokenType.Colon,
  TokenType.DoubleColon,
  TokenType.Equals,
  TokenType.NotEquals,
  TokenType.Less,
  TokenType.Greater,
  TokenType.LessOrEquals,
  TokenType.GreaterOrEquals,
  TokenType.PipeMark,
  TokenType.Concatenation,
  TokenType.At,
  TokenType.DoubleAt,
  TokenType.DollarSign,
]);

type HastNode =
  | { type: "text"; value: string }
  | {
      type: "element";
      tagName: "span";
      properties: Record<string, unknown>;
      children: HastNode[];
    };
type TransformerContext = { options: { lang: string }; source: string };

function tokenClass(
  type: number,
  text: string,
  nextSignificantType?: number,
): string | undefined {
  if (type === TokenType.Comment) return "ch-sql-com";
  if (type === TokenType.Number) return "ch-sql-num";
  if (type === TokenType.StringLiteral || type === TokenType.HereDoc)
    return "ch-sql-str";
  if (type === TokenType.QuotedIdentifier) return "ch-sql-qid";
  if (type === TokenType.BareWord) {
    if (sqlKeywords.has(text.toUpperCase())) return "ch-sql-kw";
    return nextSignificantType === TokenType.OpeningRoundBracket
      ? "ch-sql-fn"
      : "ch-sql-id";
  }
  return operatorTypes.has(type) ? "ch-sql-op" : undefined;
}

function codeChildren(code: string): HastNode[] {
  const tokens = tokenizeSync(code);
  const lines: HastNode[][] = [[]];

  for (let index = 0; index < tokens.length; index += 1) {
    const token = tokens[index];
    let nextSignificantType: number | undefined;
    for (let next = index + 1; next < tokens.length; next += 1) {
      if (isSignificant(tokens[next].type)) {
        nextSignificantType = tokens[next].type;
        break;
      }
    }
    const className = tokenClass(token.type, token.text, nextSignificantType);
    const parts = token.text.split("\n");
    for (let partIndex = 0; partIndex < parts.length; partIndex += 1) {
      if (partIndex > 0) lines.push([]);
      if (!parts[partIndex]) continue;
      const text: HastNode = { type: "text", value: parts[partIndex] };
      lines[lines.length - 1].push(
        className
          ? {
              type: "element",
              tagName: "span",
              properties: { className: [className] },
              children: [text],
            }
          : text,
      );
    }
  }

  return lines.flatMap((line, index) => [
    {
      type: "element",
      tagName: "span",
      properties: { className: ["line"] },
      children: line,
    } as HastNode,
    ...(index < lines.length - 1
      ? [{ type: "text", value: "\n" } as HastNode]
      : []),
  ]);
}

/** Re-tokenize SQL blocks with the ClickHouse server lexer before HTML exists. */
export function clickhouseSqlTransformer() {
  return {
    name: "clickhouse-sql",
    code(this: TransformerContext, node: { children: HastNode[] }) {
      if (this.options.lang.toLowerCase() !== "sql") return;
      node.children = codeChildren(this.source);
    },
  };
}
