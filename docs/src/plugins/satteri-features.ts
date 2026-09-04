import type { Features } from "satteri";

/**
 * Sätteri parser features the site relies on, shared by astro.config.ts and
 * bin/measure/mdx-compile-check.ts so the checker matches the build.
 *  - headingAttributes: `## Title {#custom-id}` (every heading in the docs).
 *  - math: `$$ ... $$` and `$...$` LaTeX as Mintlify renders it; without it the
 *    braces inside formulas are parsed as JSX expressions.
 */
export const SATTERI_FEATURES: Features = {
  headingAttributes: true,
  math: true,
};
