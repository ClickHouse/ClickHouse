#!/usr/bin/env node
/**
 * Materialise the existing Mintlify homepage JSX as React islands.
 *
 * The homepages are deliberately kept in their authored `index.mdx` files so
 * Mintlify can continue serving them until cutover. Rebuilding their large,
 * translated layouts by hand proved lossy; this generator preserves the exact
 * cards, copy, actions, imagery, and analytics attributes for Nimbus instead.
 */
import fs from "node:fs";
import path from "node:path";

const root = process.cwd();
const locales = ["en", "ar", "es", "fr", "ja", "ko", "pt-BR", "ru", "zh"];
const outputDir = path.join(root, "src", "generated", "homepage");

function sourcePath(locale: string): string {
  return locale === "en" ? path.join(root, "index.mdx") : path.join(root, locale, "index.mdx");
}

function stripFrontmatter(source: string): string {
  return source.replace(/^---\r?\n[\s\S]*?\r?\n---\r?\n/, "");
}

function splitStyle(preamble: string, file: string): { definitions: string; style: string } {
  const styleStart = preamble.indexOf("<style>");
  if (styleStart < 0) throw new Error(`Missing homepage style block in ${file}`);
  const definitions = preamble.slice(0, styleStart).trim();
  const styleSource = preamble.slice(styleStart);
  const template = styleSource.match(/^<style>\{`([\s\S]*)`\}<\/style>\s*$/);
  const plain = styleSource.match(/^<style>\s*([\s\S]*?)\s*<\/style>\s*$/);
  const style = template?.[1] ?? plain?.[1];
  if (!style) throw new Error(`Could not parse homepage style block in ${file}`);
  return { definitions, style };
}

function render(locale: string): string {
  const file = sourcePath(locale);
  const source = stripFrontmatter(fs.readFileSync(file, "utf8"));
  const pageStart = source.indexOf("<PageWrapper>");
  if (pageStart < 0) throw new Error(`Missing <PageWrapper> in ${file}`);
  const { definitions, style } = splitStyle(source.slice(0, pageStart), file);
  const body = source
    .slice(pageStart)
    .replace('id="home-search-entry"', 'id="home-search-entry" data-search-trigger')
    .trim();
  const urlLocale = locale === "en" ? "" : locale.toLowerCase();
  const assetBaseExpression = "import.meta.env.BASE_URL.replace(/\\/$/, \"\")";
  const rewrittenDefinitions = definitions
    .replaceAll(
      "if (typeof window === 'undefined') return href;",
      urlLocale
        ? `if (typeof window === 'undefined') return \`/${urlLocale}\${href}\`;`
        : "if (typeof window === 'undefined') return href;",
    )
    .replaceAll(
      "typeof window !== 'undefined' && window.location.pathname.startsWith('/docs') ? '/docs' : ''",
      assetBaseExpression,
    );
  return `// Generated from ${path.relative(root, file)}; do not edit.\n`
    + `import * as React from "react";\n`
    + `import { useEffect, useRef, useState } from "react";\n\n`
    + `${rewrittenDefinitions}\n\n`
    + `const homepageStyle = ${JSON.stringify(style)};\n\n`
    + `export default function Homepage() {\n`
    + `  return <>\n`
    + `    <style>{homepageStyle}</style>\n`
    + `    ${body}\n`
    + `  </>;\n`
    + `}\n`;
}

fs.rmSync(outputDir, { recursive: true, force: true });
fs.mkdirSync(outputDir, { recursive: true });
for (const locale of locales) {
  fs.writeFileSync(path.join(outputDir, `${locale.toLowerCase()}.jsx`), render(locale));
}
console.log(`Generated ${locales.length} exact homepage React islands.`);
