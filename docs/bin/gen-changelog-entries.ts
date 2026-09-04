#!/usr/bin/env node
/**
 * Materialise the changelog sources into a content collection without changing
 * files still served by Mintlify. OSS release files are already one release per
 * file; Cloud's current changelog groups many releases in `<Update>` blocks.
 */
import { mkdir, readdir, readFile, rm, writeFile } from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";

const docsRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const outputRoot = path.join(docsRoot, ".remote", "changelog");
const changelogRoot = path.join(docsRoot, "changelogs");
const cloudRoot = path.join(docsRoot, "resources", "changelogs", "cloud");

async function files(root: string): Promise<string[]> {
  const entries = await readdir(root, { withFileTypes: true });
  const nested = await Promise.all(entries.map(async (entry) => {
    const file = path.join(root, entry.name);
    if (entry.isDirectory()) return files(file);
    return entry.isFile() && entry.name.endsWith(".md") ? [file] : [];
  }));
  return nested.flat().sort();
}

function yamlString(value: string): string {
  return JSON.stringify(value);
}

function escapeLiteralMdxBraces(body: string): string {
  // Release notes are legacy Markdown, not MDX: braces represent SQL macros,
  // log placeholders, or prose. Escape them while materialising the separate
  // collection so historical text cannot become a JSX expression at build time.
  return body.replaceAll("{", "&#123;").replaceAll("}", "&#125;");
}

function escapeOssMdxLiterals(body: string): string {
  // The OSS files are Markdown-only and several historical releases include
  // shell/log placeholders such as `<db>`. Unlike Cloud entries, they have no
  // MDX components to retain, so angle brackets are literals as well.
  return escapeLiteralMdxBraces(body).replaceAll("<", "&lt;").replaceAll(">", "&gt;");
}

function ossFrontmatter(file: string): string {
  const release = path.basename(file, ".md");
  const match = /^v(?<version>[\d.]+)-(?<channel>[^.]+)$/.exec(release);
  if (!match?.groups) throw new Error(`Unexpected changelog filename: ${file}`);
  const version = match.groups.version;
  const channel = match.groups.channel;
  const [major, minor] = version.split(".");
  if (!major || !minor || Number(minor) < 1 || Number(minor) > 12) {
    throw new Error(`Cannot derive release month from ${release}`);
  }
  // Changelog file names encode a release train (for example, 24.8). Their
  // sources contain no publication day, so use the first day of that month as
  // a stable sort key rather than making a false claim about an exact day.
  const date = `20${major.padStart(2, "0")}-${minor.padStart(2, "0")}-01`;
  return [
    "---",
    `title: ${yamlString(`ClickHouse ${version}`)}`,
    `description: ${yamlString(`Release notes for ClickHouse ${version} (${channel}).`)}`,
    `date: ${yamlString(date)}`,
    "products: [\"ClickHouse\"]",
    `channel: ${yamlString(channel)}`,
    "hidden: false",
    "---",
    "",
  ].join("\n");
}

function cloudDate(label: string): string {
  if (/^\d{4}-\d{2}-\d{2}$/.test(label)) return label;
  const parsed = new Date(`${label} UTC`);
  if (Number.isNaN(parsed.valueOf())) throw new Error(`Unparseable Cloud update date: ${label}`);
  return parsed.toISOString().slice(0, 10);
}

function cloudTitle(body: string, date: string): string {
  const heading = /^#{1,6}\s+(.+?)(?:\s+\{#[^}]+\})?\s*$/m.exec(body)?.[1];
  return heading?.replace(/\s+\{#[^}]+\}\s*$/, "") ?? `ClickHouse Cloud update — ${date}`;
}

async function writeOssEntries(): Promise<number> {
  const sourceFiles = await files(changelogRoot);
  let count = 0;
  for (const source of sourceFiles) {
    const entryId = path.relative(changelogRoot, source).replace(/\.md$/, "");
    const output = path.join(outputRoot, "oss", `${entryId}.mdx`);
    await mkdir(path.dirname(output), { recursive: true });
    await writeFile(output, ossFrontmatter(source) + escapeOssMdxLiterals(await readFile(source, "utf8")));
    count++;
  }
  return count;
}

async function writeCloudEntries(): Promise<number> {
  const current = await readFile(path.join(cloudRoot, "2026.mdx"), "utf8");
  const updates = [...current.matchAll(/<Update\s+label="([^"]+)"[^>]*>\s*([\s\S]*?)\s*<\/Update>/g)];
  if (!updates.length) throw new Error("No Cloud <Update> blocks found");
  await mkdir(path.join(outputRoot, "cloud"), { recursive: true });
  for (const [index, update] of updates.entries()) {
    const [, label, body] = update;
    const date = cloudDate(label);
    const id = `${date}-${String(index + 1).padStart(2, "0")}`;
    const hasImage = /<Image\b/.test(body);
    const frontmatter = [
      "---",
      `title: ${yamlString(cloudTitle(body, date))}`,
      `description: ${yamlString(`ClickHouse Cloud update published ${date}.`)}`,
      `date: ${yamlString(date)}`,
      "products: [\"ClickHouse Cloud\"]",
      "hidden: false",
      "---",
      "",
      ...(hasImage ? ["import { Image } from \"/snippets/components/Image.jsx\";", ""] : []),
    ].join("\n");
    await writeFile(path.join(outputRoot, "cloud", `${id}.mdx`), frontmatter + escapeLiteralMdxBraces(body.trim()) + "\n");
  }
  return updates.length;
}

await rm(outputRoot, { recursive: true, force: true });
await mkdir(outputRoot, { recursive: true });
const [oss, cloud] = await Promise.all([writeOssEntries(), writeCloudEntries()]);
console.log(`Generated ${oss} OSS and ${cloud} ClickHouse Cloud changelog entries.`);
