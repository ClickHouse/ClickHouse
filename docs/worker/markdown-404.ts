/**
 * Markdown-aware 404. A client that asked for Markdown (via `/index.md` or
 * `Accept: text/markdown`) gets a short Markdown body with recovery links
 * instead of the HTML page; the status stays a real 404.
 */
const BASE = "/docs";

export function requestsMarkdown(request: Request): boolean {
  const { pathname } = new URL(request.url);
  if (pathname.endsWith(".md") || pathname.endsWith(".mdx")) return true;
  return (request.headers.get("Accept") ?? "")
    .split(",")
    .some((type) => type.split(";")[0].trim().toLowerCase() === "text/markdown");
}

export function markdownNotFound(): Response {
  const body = `# 404 Page not found

The page you requested does not exist or has moved.

Browse the documentation via [llms.txt](${BASE}/llms.txt).
`;
  return new Response(body, { status: 404, headers: { "Content-Type": "text/markdown; charset=utf-8" } });
}
