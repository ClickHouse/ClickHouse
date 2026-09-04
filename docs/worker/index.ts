/**
 * clickhouse-docs Worker: serves the Astro/Nimbus build (static assets nested
 * under /docs) with
 *  - 13.6k redirects from `__redirects` (beyond the static-asset rule limits),
 *  - markdown for agents: `Accept: text/markdown` or `.md`/`.mdx` -> `index.md`,
 *  - a markdown-aware 404.
 * Reached in production through the website Worker's Service Binding.
 */
import { WorkerEntrypoint } from "cloudflare:workers";
import redirectsFile from "../dist/__redirects";
import { markdownNotFound, requestsMarkdown } from "./markdown-404";
import { createRedirectEvaluator } from "./redirects";

interface Env {
  ASSETS: Fetcher;
}

const BASE = "/docs";
const redirectsEvaluator = createRedirectEvaluator(redirectsFile);

/** Same-origin redirects for a markdown request should stay in markdown. */
function rewriteRedirectForMarkdown(redirect: Response, requestUrl: URL): Response {
  const location = redirect.headers.get("Location");
  if (!location) return redirect;
  const dest = new URL(location, requestUrl.origin);
  if (dest.origin !== requestUrl.origin || !dest.pathname.startsWith(BASE)) return redirect;
  dest.pathname = dest.pathname.replace(/\/?$/, "/") + "index.md";
  const headers = new Headers(redirect.headers);
  headers.set("Location", dest.pathname + dest.search);
  return new Response(redirect.body, { status: redirect.status, headers });
}

export default class extends WorkerEntrypoint<Env> {
  override async fetch(request: Request): Promise<Response> {
    const url = new URL(request.url);
    const wantsMarkdown = requestsMarkdown(request);

    // Redirects are authored for HTML paths; evaluate the markdown twin's page path.
    const pagePath = url.pathname.replace(/\/index\.mdx?$/, "/").replace(/\.mdx?$/, "/");
    for (const candidate of [pagePath, pagePath.endsWith("/") ? pagePath : pagePath + "/"]) {
      const evalRequest = new Request(url.origin + candidate + url.search, request);
      const redirect = redirectsEvaluator.redirect(evalRequest);
      if (redirect) return wantsMarkdown ? rewriteRedirectForMarkdown(redirect, url) : redirect;
    }

    if (wantsMarkdown) {
      // `.mdx` twins are pruned from the build; serve the `.md` twin for both.
      let mdPath = url.pathname;
      if (mdPath.endsWith("/index.mdx")) mdPath = mdPath.slice(0, -1);
      else if (!mdPath.endsWith(".md")) mdPath = mdPath.replace(/\/?$/, "/") + "index.md";
      const md = await this.env.ASSETS.fetch(new Request(url.origin + mdPath, request));
      if (md.ok) {
        const headers = new Headers(md.headers);
        headers.set("Content-Type", "text/markdown; charset=utf-8");
        headers.set("Vary", "Accept");
        return new Response(md.body, { status: md.status, headers });
      }
      return markdownNotFound();
    }

    const response = await this.env.ASSETS.fetch(request);
    if (response.status === 404) {
      const notFound = await this.env.ASSETS.fetch(new Request(`${url.origin}${BASE}/404.html`, request));
      return new Response(notFound.body, { status: 404, headers: notFound.headers });
    }
    return response;
  }
}
