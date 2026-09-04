/**
 * Build scope: which locales to build and whether the reference section is
 * included. Workers Builds cannot pass per-build environment variables, so
 * Praktika commits `docs/.preview-scope.json` on the mirror branch; environment
 * variables (local builds, Praktika itself) override the file.
 *
 *   { "locales": ["en", "es"], "reference": false }
 */
import fs from "node:fs";
import path from "node:path";

export const ALL_LOCALES = ["ar", "es", "fr", "ja", "ko", "pt-BR", "ru", "zh"] as const;
export type Locale = (typeof ALL_LOCALES)[number];

export interface BuildScope {
  /** Locale trees to build (English is always built). */
  locales: Locale[];
  /** Whether `reference/**` is part of the build. */
  reference: boolean;
  source: "env" | "file" | "default";
}

export function readScope(root = process.cwd()): BuildScope {
  const envLocales = (process.env.DOCS_LOCALES ?? "").trim();
  const envReference = (process.env.DOCS_REFERENCE ?? "").trim().toLowerCase();
  const file = path.join(root, ".preview-scope.json");
  let fileScope: { locales?: string[]; reference?: boolean } | null = null;
  if (fs.existsSync(file)) fileScope = JSON.parse(fs.readFileSync(file, "utf8"));

  const parseLocales = (list: string[]): Locale[] =>
    list.filter((l): l is Locale => (ALL_LOCALES as readonly string[]).includes(l));

  let locales: Locale[];
  let source: BuildScope["source"] = "default";
  if (envLocales) {
    locales = envLocales === "all" ? [...ALL_LOCALES] : parseLocales(envLocales.split(",").map((s) => s.trim()));
    source = "env";
  } else if (fileScope?.locales) {
    locales = parseLocales(fileScope.locales);
    source = "file";
  } else {
    locales = [];
  }
  const reference = envReference ? envReference !== "off" : fileScope?.reference ?? true;
  return { locales, reference, source };
}
