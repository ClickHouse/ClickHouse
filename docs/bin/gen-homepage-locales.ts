/** Extract the native homepage copy from the existing English and locale MDX. */
import fs from "node:fs";
import path from "node:path";

type Card = { title: string; description: string; href: string };
type Homepage = {
  title: string;
  headline: string;
  subtitle: string;
  cards: Card[];
};

const root = process.cwd();
const locales = ["en", "ar", "es", "fr", "ja", "ko", "pt-BR", "ru", "zh"];

function clean(value: string): string {
  return value
    .replace(/<HighlightedClickHouse\s*\/>/g, "ClickHouse")
    .replace(/<[^>]+>/g, " ")
    .replace(/\{[^}]+\}/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function sourceFor(locale: string): string {
  const file =
    locale === "en"
      ? path.join(root, "index.mdx")
      : path.join(root, locale, "index.mdx");
  return fs.readFileSync(file, "utf8");
}

function parse(locale: string): Homepage {
  const source = sourceFor(locale);
  const frontmatter = source.match(/^---\r?\n([\s\S]*?)\r?\n---/);
  const title =
    frontmatter?.[1].match(/^title:\s*["']?(.+?)["']?\s*$/m)?.[1] ??
    "ClickHouse Docs";
  const headings = [...source.matchAll(/<h1[^>]*>([\s\S]*?)<\/h1>/g)].map(
    (match) => clean(match[1]),
  );
  const headline = headings.find(Boolean) ?? title;
  const afterHeadline = source.slice((source.indexOf("</h1>") || 0) + 5);
  const subtitleMatch = afterHeadline.match(
    /<div[^>]*tracking-tight[^>]*>([\s\S]*?)<\/div>/,
  );
  const subtitle = clean(subtitleMatch?.[1] ?? "");
  const cards = [
    ...source.matchAll(
      /<HeroCard\s+title="([^"]+)"[\s\S]*?description="([^"]*)"[\s\S]*?href=\{localizeHref\("([^"]+)"\)\}/g,
    ),
  ]
    .slice(0, 4)
    .map((match) => ({
      title: match[1],
      description: match[2],
      href: match[3],
    }));
  if (cards.length !== 4)
    throw new Error(
      `Expected four primary cards in ${locale}/index.mdx, found ${cards.length}.`,
    );
  return { title, headline, subtitle, cards };
}

const output = Object.fromEntries(
  locales.map((locale) => [locale, parse(locale)]),
);
const file = path.join(root, "src/generated/homepage-locales.json");
fs.mkdirSync(path.dirname(file), { recursive: true });
fs.writeFileSync(file, `${JSON.stringify(output, null, 2)}\n`);
