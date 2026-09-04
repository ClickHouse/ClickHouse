/**
 * Materialise the remote OpenAPI documents used by the static API explorer.
 *
 * The renderer only reads local files, which keeps page generation
 * deterministic and ensures a failed or invalid download stops the build.
 */
import { mkdir, rename, rm, writeFile } from "node:fs/promises";
import path from "node:path";

const root = process.cwd();
const outputDirectory = path.join(root, ".remote", "specs");

const specifications = [
  {
    name: "cloud",
    url: "https://api.clickhouse.cloud/v1",
    output: "cloud-openapi.json",
  },
  {
    name: "clickstack",
    url: "https://raw.githubusercontent.com/hyperdxio/hyperdx/refs/heads/main/packages/api/openapi.json",
    output: "clickstack-openapi.json",
  },
] as const;

async function fetchSpecification(specification: (typeof specifications)[number]): Promise<void> {
  const response = await fetch(specification.url, {
    headers: { Accept: "application/json" },
    signal: AbortSignal.timeout(60_000),
  });
  if (!response.ok) {
    throw new Error(`fetch-openapi: ${specification.name} returned HTTP ${response.status}`);
  }

  const document = await response.json() as Record<string, unknown>;
  if (typeof document.openapi !== "string" || !document.paths || typeof document.paths !== "object") {
    throw new Error(`fetch-openapi: ${specification.name} did not return an OpenAPI document`);
  }

  const destination = path.join(outputDirectory, specification.output);
  const temporary = `${destination}.new`;
  await writeFile(temporary, `${JSON.stringify(document)}\n`, "utf8");
  await rename(temporary, destination);
  console.log(`fetch-openapi: ${specification.name} -> ${path.relative(root, destination)}`);
}

await mkdir(outputDirectory, { recursive: true });
try {
  await Promise.all(specifications.map(fetchSpecification));
} catch (error) {
  await Promise.all(specifications.map(({ output }) => rm(path.join(outputDirectory, `${output}.new`), { force: true })));
  throw error;
}
