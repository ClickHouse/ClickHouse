import type { ApiAuthView, ApiFieldView } from "@cloudflare/nimbus-docs/api";

export interface ApiSecurityScheme {
  type?: string;
  scheme?: string;
  name?: string;
  in?: string;
  description?: string;
}

interface AuthFieldView extends ApiFieldView {
  location?: string;
}

function authTypeLabel(a: ApiAuthView, definition?: ApiSecurityScheme): string {
  if (a.type === "http") {
    return definition?.scheme === "basic" ? "string" : a.bearerFormat ? `Bearer ${a.bearerFormat}` : "string";
  }
  if (a.type === "apiKey") return "string";
  if (a.type === "oauth2") return "OAuth 2.0";
  if (a.type === "openIdConnect") return "OpenID Connect";
  if (a.type === "mutualTLS") return "Mutual TLS";
  return a.type ?? a.scheme;
}

function authExample(a: ApiAuthView): string | null {
  if (!a.headerName) return null;
  const prefix = a.type === "http" && a.bearerFormat ? "Bearer " : "";
  return `${a.headerName}: ${prefix}<token>`;
}

// OpenAPI restricts scheme keys to [A-Za-z0-9._-]; belt-and-braces for a
// non-conformant spec.
const authAnchor = (scheme: string): string =>
  `auth-${scheme.replace(/[^A-Za-z0-9._-]/g, "-")}`;

interface AuthAgg {
  view: ApiAuthView;
  definition?: ApiSecurityScheme;
  scopes: Set<string>;
  required: boolean;
}

function escapeHtml(value: string): string {
  return value.replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;").replace(/"/g, "&quot;");
}

function descriptionHtml(value?: string): string | undefined {
  if (!value) return undefined;
  return escapeHtml(value).replace(
    /(https:\/\/[^\s<]+)/g,
    '<a href="$1">$1</a>',
  );
}

function toField(agg: AuthAgg): AuthFieldView {
  const a = agg.view;
  const definition = agg.definition;
  const scopes = [...agg.scopes];
  const example = authExample(a);
  const parts = [
    example ? `\`${example}\`` : "",
    scopes.length > 0 ? `Scopes: ${scopes.map((s) => `\`${s}\``).join(", ")}` : "",
  ].filter(Boolean);
  return {
    coordinate: `auth:${a.scheme}`,
    name: a.headerName ?? definition?.name ?? (a.type === "http" ? "Authorization" : a.scheme),
    type: authTypeLabel(a, definition),
    location: a.in ?? definition?.in ?? (a.type === "http" ? "header" : undefined),
    required: agg.required,
    anchor: authAnchor(a.scheme),
    children: [],
    childCount: 0,
    truncated: false,
    description: definition?.description ?? (parts.length > 0 ? parts.join(" · ") : undefined),
    descriptionHtml: descriptionHtml(definition?.description ?? (parts.length > 0 ? parts.join(" · ") : undefined)),
  };
}

// Every distinct scheme across the alternatives as one field list. `auth` is
// `ApiAuthView[][]` (outer OR, inner AND): a scheme is `required` only when it
// appears in EVERY alternative, and scopes are unioned. First-seen order.
export function authFields(
  auth: ApiAuthView[][],
  definitions: Record<string, ApiSecurityScheme> = {},
): ApiFieldView[] {
  const order: string[] = [];
  const byScheme = new Map<string, AuthAgg>();
  for (const alt of auth) {
    for (const a of alt) {
      let agg = byScheme.get(a.scheme);
      if (!agg) {
        agg = { view: a, definition: definitions[a.scheme], scopes: new Set(), required: true };
        byScheme.set(a.scheme, agg);
        order.push(a.scheme);
      }
      for (const s of a.scopes) agg.scopes.add(s);
    }
  }
  for (const [scheme, agg] of byScheme) {
    agg.required = auth.every((alt) => alt.some((a) => a.scheme === scheme));
  }
  return order.map((s) => toField(byScheme.get(s)!));
}
