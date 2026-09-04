/** Extract the established ClickHouse SQL keyword table for build-time Shiki. */
import fs from "node:fs";
import path from "node:path";

const root = process.cwd();
const source = fs.readFileSync(
  path.join(root, "_site/customizations/clickhouse-sql-highlight.js"),
  "utf8",
);
const match = source.match(/var SQL_KEYWORDS = new Set\(\[([\s\S]*?)\]\);/);
if (!match)
  throw new Error("ClickHouse SQL keyword table changed unexpectedly.");

const keywords = [...match[1].matchAll(/'([^']+)'/g)].map((entry) => entry[1]);
if (keywords.length === 0)
  throw new Error("ClickHouse SQL keyword table is empty.");

const output = path.join(root, "src/generated/clickhouse-sql-keywords.json");
fs.mkdirSync(path.dirname(output), { recursive: true });
fs.writeFileSync(output, `${JSON.stringify(keywords, null, 2)}\n`);
