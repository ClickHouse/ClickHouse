import fs from "node:fs";
import path from "node:path";
import {
  buildApiModel,
  getApiPageProps,
  renderApiPageMarkdown as renderNimbusApiPageMarkdown,
  type ApiModel,
  type ApiOperationPage as NimbusApiOperationPage,
} from "@cloudflare/nimbus-docs/api";

export const HTTP_METHODS = ["get", "post", "put", "patch", "delete", "head", "options"] as const;
export type HttpMethod = (typeof HTTP_METHODS)[number];
export type ApiCollection = "cloud" | "clickstack";

export interface OpenApiSchema {
  $ref?: string;
  type?: string | string[];
  format?: string;
  title?: string;
  description?: string;
  nullable?: boolean;
  required?: string[];
  properties?: Record<string, OpenApiSchema>;
  items?: OpenApiSchema;
  enum?: unknown[];
  example?: unknown;
  examples?: unknown[];
  default?: unknown;
  allOf?: OpenApiSchema[];
  oneOf?: OpenApiSchema[];
  anyOf?: OpenApiSchema[];
  additionalProperties?: boolean | OpenApiSchema;
  [key: string]: unknown;
}

export interface OpenApiParameter {
  $ref?: string;
  name?: string;
  in?: string;
  description?: string;
  required?: boolean;
  schema?: OpenApiSchema;
  example?: unknown;
}

export interface OpenApiMediaType {
  schema?: OpenApiSchema;
  example?: unknown;
  examples?: Record<string, { value?: unknown }>;
}

export interface OpenApiResponse {
  $ref?: string;
  description?: string;
  content?: Record<string, OpenApiMediaType>;
}

export interface OpenApiOperation {
  summary?: string;
  description?: string;
  operationId?: string;
  tags?: string[];
  deprecated?: boolean;
  parameters?: OpenApiParameter[];
  requestBody?: {
    required?: boolean;
    description?: string;
    content?: Record<string, OpenApiMediaType>;
  };
  responses?: Record<string, OpenApiResponse>;
  security?: Array<Record<string, string[]>>;
  [key: string]: unknown;
}

export interface OpenApiDocument {
  openapi: string;
  info?: { title?: string; description?: string; version?: string };
  servers?: Array<{ url: string; description?: string }>;
  paths: Record<string, Partial<Record<HttpMethod, OpenApiOperation>> & { parameters?: OpenApiParameter[] }>;
  components?: {
    schemas?: Record<string, OpenApiSchema>;
    parameters?: Record<string, OpenApiParameter>;
    responses?: Record<string, OpenApiResponse>;
    securitySchemes?: Record<string, Record<string, unknown>>;
  };
  security?: Array<Record<string, string[]>>;
}

export interface ApiOperationPage {
  collection: ApiCollection;
  method: HttpMethod;
  path: string;
  tag: string;
  tagSlug: string;
  slug: string;
  route: string;
  title: string;
  operation: OpenApiOperation;
  pathParameters: OpenApiParameter[];
  document: OpenApiDocument;
  sourceUrl: string;
  editUrl?: string;
  badge?: string;
}

const COLLECTIONS: Record<ApiCollection, { file: string; directory: string; sourceUrl: string; editUrl?: string }> = {
  cloud: {
    file: ".remote/specs/cloud-openapi.json",
    directory: "products/cloud/api-reference",
    sourceUrl: "https://api.clickhouse.cloud/v1",
    editUrl: "https://github.com/ClickHouse/ClickHouse/edit/master/docs/_specs/cloud-openapi.json",
  },
  clickstack: {
    file: ".remote/specs/clickstack-openapi.json",
    directory: "clickstack/api-reference",
    sourceUrl: "https://raw.githubusercontent.com/hyperdxio/hyperdx/refs/heads/main/packages/api/openapi.json",
    editUrl: "https://github.com/hyperdxio/hyperdx/edit/main/packages/api/openapi.json",
  },
};

const documentCache = new Map<ApiCollection, OpenApiDocument>();
const operationCache = new Map<ApiCollection, ApiOperationPage[]>();
const nativeModelCache = new Map<ApiCollection, Promise<ApiModel>>();

export interface RoutedApiOperationPage {
  source: ApiOperationPage;
  view: NimbusApiOperationPage;
}

export function slugifyApiSegment(value: string): string {
  return value
    .normalize("NFKD")
    .toLowerCase()
    .replace(/['’]/g, "")
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/(^-|-$)/g, "") || "operation";
}

export function parseOperationPointer(pointer: string): { method: HttpMethod; path: string } {
  const match = pointer.trim().match(/^(GET|POST|PUT|PATCH|DELETE|HEAD|OPTIONS)\s+(\/\S+)$/i);
  if (!match) throw new Error(`Invalid OpenAPI operation pointer: ${pointer}`);
  return { method: match[1].toLowerCase() as HttpMethod, path: match[2] };
}

function docsRoot(): string {
  return process.cwd();
}

export function loadOpenApiDocument(collection: ApiCollection): OpenApiDocument {
  const cached = documentCache.get(collection);
  if (cached) return cached;
  const file = path.join(docsRoot(), COLLECTIONS[collection].file);
  if (!fs.existsSync(file)) {
    throw new Error(`OpenAPI specification missing at ${file}. Run \`pnpm predev\` or \`pnpm prebuild\`.`);
  }
  const document = JSON.parse(fs.readFileSync(file, "utf8")) as OpenApiDocument;
  if (typeof document.openapi !== "string" || !document.paths) {
    throw new Error(`Invalid OpenAPI specification at ${file}`);
  }
  documentCache.set(collection, document);
  return document;
}

function explicitCloudSlugs(): Map<string, { route: string; badge?: string }> {
  const routes = new Map<string, { route: string; badge?: string }>();
  const directory = path.join(docsRoot(), "products", "cloud", "api-reference");
  if (!fs.existsSync(directory)) return routes;
  const visit = (current: string) => {
    for (const entry of fs.readdirSync(current, { withFileTypes: true })) {
      const full = path.join(current, entry.name);
      if (entry.isDirectory()) visit(full);
      else if (/\.mdx?$/.test(entry.name)) {
        const source = fs.readFileSync(full, "utf8");
        const pointer = source.match(/^openapi:\s*["']?(?:\/_specs\/cloud-openapi\.json\s+)?(GET|POST|PUT|PATCH|DELETE|HEAD|OPTIONS)\s+(\/[^"'\r\n]+)["']?\s*$/mi);
        if (!pointer) continue;
        const key = `${pointer[1].toLowerCase()} ${pointer[2].trim()}`;
        const badge = source.match(/^tag:\s*["']?([^"'\r\n]+)["']?\s*$/mi)?.[1]?.trim();
        routes.set(key, {
          route: path.relative(path.join(docsRoot(), COLLECTIONS.cloud.directory), full).replace(/\.mdx?$/, "").split(path.sep).join("/"),
          badge,
        });
      }
    }
  };
  visit(directory);
  return routes;
}

export function getApiOperations(collection: ApiCollection): ApiOperationPage[] {
  const cached = operationCache.get(collection);
  if (cached) return cached;
  const document = loadOpenApiDocument(collection);
  const config = COLLECTIONS[collection];
  const explicitRoutes = collection === "cloud"
    ? explicitCloudSlugs()
    : new Map<string, { route: string; badge?: string }>();
  const operations: ApiOperationPage[] = [];
  const seenRoutes = new Set<string>();

  for (const [apiPath, pathItem] of Object.entries(document.paths)) {
    for (const method of HTTP_METHODS) {
      const operation = pathItem[method];
      if (!operation) continue;
      const tag = operation.tags?.[0] ?? "Other";
      const key = `${method} ${apiPath}`;
      const explicit = explicitRoutes.get(key);
      const tagSlug = explicit?.route.includes("/") ? explicit.route.split("/")[0] : slugifyApiSegment(tag);
      const slug = explicit?.route.includes("/") ? explicit.route.split("/").slice(1).join("/") : slugifyApiSegment(operation.summary ?? operation.operationId ?? `${method}-${apiPath}`);
      const route = `${config.directory}/${tagSlug}/${slug}`;
      if (seenRoutes.has(route)) throw new Error(`Duplicate OpenAPI route: ${route}`);
      seenRoutes.add(route);
      operations.push({
        collection,
        method,
        path: apiPath,
        tag,
        tagSlug,
        slug,
        route,
        title: operation.summary ?? operation.operationId ?? `${method.toUpperCase()} ${apiPath}`,
        operation,
        pathParameters: [...(pathItem.parameters ?? []), ...(operation.parameters ?? [])],
        document,
        sourceUrl: config.sourceUrl,
        editUrl: config.editUrl,
        badge: explicit?.badge,
      });
    }
  }

  operationCache.set(collection, operations);
  return operations;
}

export function getApiOperation(collection: ApiCollection, tagSlug: string, operationSlug: string): ApiOperationPage {
  const operation = getApiOperations(collection).find((candidate) => candidate.tagSlug === tagSlug && candidate.slug === operationSlug);
  if (!operation) throw new Error(`Unknown ${collection} OpenAPI operation: ${tagSlug}/${operationSlug}`);
  return operation;
}

export function getApiOperationByPointer(collection: ApiCollection, pointer: string): ApiOperationPage {
  const { method, path: apiPath } = parseOperationPointer(pointer);
  const operation = getApiOperations(collection).find((candidate) => candidate.method === method && candidate.path === apiPath);
  if (!operation) throw new Error(`OpenAPI operation not found in ${collection}: ${pointer}`);
  return operation;
}

async function getNativeApiModel(collection: ApiCollection): Promise<ApiModel> {
  const cached = nativeModelCache.get(collection);
  if (cached) return cached;

  const config = COLLECTIONS[collection];
  const document = structuredClone(loadOpenApiDocument(collection)) as OpenApiDocument & { "x-tagGroups"?: unknown };
  // ClickHouse's tag groups intentionally use the same label as their first
  // child. The docs sidebar preserves that Mintlify hierarchy; Nimbus's API
  // model does not need it here and otherwise reports self-parent warnings.
  delete document["x-tagGroups"];

  const promise = buildApiModel({
    collection: `${collection}-api`,
    spec: JSON.stringify(document),
    mountPath: `/docs/${config.directory}`,
    requireOperationId: true,
  });
  nativeModelCache.set(collection, promise);
  promise.catch(() => {
    if (nativeModelCache.get(collection) === promise) nativeModelCache.delete(collection);
  });
  return promise;
}

export async function getRoutedApiOperation(
  collection: ApiCollection,
  tagSlug: string,
  operationSlug: string,
): Promise<RoutedApiOperationPage> {
  const source = getApiOperation(collection, tagSlug, operationSlug);
  const coordinate = source.operation.operationId;
  if (!coordinate) throw new Error(`OpenAPI operation is missing operationId: ${source.method.toUpperCase()} ${source.path}`);

  const page = getApiPageProps(await getNativeApiModel(collection), coordinate);
  if (page.kind !== "operation") throw new Error(`OpenAPI coordinate is not an operation: ${coordinate}`);

  const href = `/docs/${source.route}`;
  return {
    source,
    view: {
      ...page,
      href,
      markdownHref: `${href}/index.md`,
    },
  };
}

export function apiMethodVariant(method: HttpMethod): "success" | "info" | "warning" | "danger" | "note" {
  if (method === "get") return "success";
  if (method === "post") return "info";
  if (method === "delete") return "danger";
  if (method === "patch") return "warning";
  return "note";
}

export async function renderApiOperationMarkdown(page: ApiOperationPage): Promise<string> {
  const routed = await getRoutedApiOperation(page.collection, page.tagSlug, page.slug);
  return `${renderNimbusApiPageMarkdown(routed.view).trim()}\n\nOpenAPI source: ${page.sourceUrl}\n`;
}
