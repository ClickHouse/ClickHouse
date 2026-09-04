/**
 * Evaluate the generated `__redirects` file in the Worker.
 *
 * `redirects-in-workers` delegates to the platform parser, whose dynamic-rule
 * cap makes it stop reading this 14k-rule file after the first 200 entries.
 * Our generated file has unique sources and only four splat rules, so a small
 * pre-parsed evaluator is both complete and predictable.
 */
interface RedirectRule {
  destination: string;
  status: number;
}

interface SplatRule extends RedirectRule {
  prefix: string;
}

export interface RedirectEvaluator {
  redirect(request: Request): Response | null;
}

export function createRedirectEvaluator(contents: string): RedirectEvaluator {
  const staticRules = new Map<string, RedirectRule>();
  const splatRules: SplatRule[] = [];

  for (const rawLine of contents.split("\n")) {
    const line = rawLine.trim();
    if (!line || line.startsWith("#")) continue;
    const [source, destination, statusText] = line.split(/\s+/);
    const status = Number(statusText ?? "302");
    if (!source || !destination || !Number.isInteger(status)) {
      throw new Error(`Invalid redirect rule: ${rawLine}`);
    }
    if (source.endsWith("/*")) {
      splatRules.push({ prefix: source.slice(0, -1), destination, status });
    } else {
      staticRules.set(source, { destination, status });
    }
  }

  function responseFor(rule: RedirectRule, requestUrl: URL, splat = ""): Response {
    const destination = new URL(rule.destination.replace(":splat", splat), requestUrl);
    const location = destination.origin === requestUrl.origin
      ? `${destination.pathname}${destination.search || requestUrl.search}${destination.hash}`
      : `${destination.href.slice(0, destination.href.length - destination.search.length - destination.hash.length)}${destination.search || requestUrl.search}${destination.hash}`;
    return new Response(null, { status: rule.status, headers: { Location: location } });
  }

  return {
    redirect(request: Request): Response | null {
      const requestUrl = new URL(request.url);
      const staticRule = staticRules.get(requestUrl.pathname);
      if (staticRule) return responseFor(staticRule, requestUrl);
      for (const rule of splatRules) {
        if (requestUrl.pathname.startsWith(rule.prefix)) {
          return responseFor(rule, requestUrl, requestUrl.pathname.slice(rule.prefix.length));
        }
      }
      return null;
    },
  };
}
