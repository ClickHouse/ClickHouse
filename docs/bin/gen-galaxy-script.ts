/**
 * Prepare the legacy Galaxy implementation as a Nimbus static asset.
 *
 * Its event envelope is shared with the marketing site, so retain the tested
 * implementation verbatim except for recognising Workers preview hosts.
 */
import fs from "node:fs";
import path from "node:path";

const root = process.cwd();
const input = path.join(root, "_site/customizations/galaxy.js");
const output = path.join(root, "src/generated/galaxy.js");
let source = fs.readFileSync(input, "utf8");

const previewDeclaration =
  "var isMintlifyPreview = /\\.mintlify\\.app$/\n    .test(window.location.hostname || '');";
if (!source.includes(previewDeclaration))
  throw new Error("Galaxy preview-host declaration changed unexpectedly.");
source = source.replace(
  previewDeclaration,
  `${previewDeclaration}\n  // Workers Builds previews use an aliased workers.dev hostname.\n  var isWorkersPreview = /\\.workers\\.dev$/.test(window.location.hostname || '');`,
);

const previewCondition = "if (!isLocal && isMintlifyPreview) {";
if (!source.includes(previewCondition))
  throw new Error("Galaxy preview-host condition changed unexpectedly.");
source = source.replace(
  previewCondition,
  "if (!isLocal && (isMintlifyPreview || isWorkersPreview)) {",
);

fs.mkdirSync(path.dirname(output), { recursive: true });
fs.writeFileSync(output, source);
