/**
 * Type-display helpers for the API field explorer — pure projection of an
 * `ApiFieldView` into the coloured type preview (keyword vs literal vs link),
 * the collapsed-object marker, and the constraint pairs. Kept out of the
 * `.astro` files so the formatting has one home and can be unit-tested without
 * a render. Palette is deliberately restrained: muted for everything except a
 * schema link (`text-info`); no syntax colours.
 */
import type { ApiConstraint, ApiFieldView, ApiUnionView } from "@cloudflare/nimbus-docs/api";

export interface TypeToken {
  text: string;
  /** Tailwind classes — semantic tokens so light/dark track automatically. */
  cls: string;
  href?: string;
  /** Monospace token — `$ref` names and literals render in mono; the property
   *  name, `optional`, punctuation, and primitives in sans. */
  mono?: boolean;
  /** Native tooltip — carries the full enum list behind a `+N more` token. */
  title?: string;
}

/** Blue link — a type that resolves to a schema page (rendered in mono). */
export const LINK_CLS = "text-info font-medium hover:underline underline-offset-2";
const MUTED_CLS = "text-muted-foreground";

/** How many enum literals to show inline before collapsing to `+N more`. */
const ENUM_CAP = 6;

// Enum literals render bare (not JSON-quoted) — cleaner in a docs reading
// context, and the muted mono already marks them as values.
const display = (v: unknown): string => (typeof v === "string" ? v : String(v));

function literalTokens(values: readonly unknown[]): TypeToken[] {
  const out: TypeToken[] = [];
  values.slice(0, ENUM_CAP).forEach((v, i) => {
    if (i > 0) out.push({ text: "or", cls: MUTED_CLS });
    out.push({ text: display(v), cls: MUTED_CLS, mono: true });
  });
  if (values.length > ENUM_CAP) {
    out.push({
      text: `+${values.length - ENUM_CAP} more`,
      cls: MUTED_CLS,
      title: values.map(display).join(", "),
    });
  }
  return out;
}

function variantTokens(union: ApiUnionView): TypeToken[] {
  const variants =
    union.mapping && union.mapping.length > 0
      ? union.mapping.map((m) => m.variant)
      : union.variants;
  const out: TypeToken[] = [];
  variants.forEach((v, i) => {
    if (i > 0) out.push({ text: "or", cls: MUTED_CLS });
    out.push({ text: v.label, cls: v.href ? LINK_CLS : MUTED_CLS, href: v.href, mono: Boolean(v.href) });
  });
  return out;
}

/** The coloured type preview for a field, as a token stream. Separators carry
 *  no baked whitespace — the renderer's `gap` supplies the spacing. */
export function typeTokens(field: ApiFieldView): TypeToken[] {
  const shape = field.typeShape;

  if (shape?.kind === "map") {
    return [
      { text: "map of", cls: MUTED_CLS },
      field.typeRef
        ? { text: shape.inner, cls: LINK_CLS, href: field.typeRef.href, mono: true }
        : { text: shape.inner || "any", cls: MUTED_CLS },
    ];
  }

  const prefix: TypeToken[] = shape?.kind === "array" ? [{ text: "array of", cls: MUTED_CLS }] : [];

  if (field.union) return [...prefix, ...variantTokens(field.union)];

  if (shape?.kind === "array") {
    if (field.enum && field.enum.length > 0) return [...prefix, ...literalTokens(field.enum)];
    if (field.typeRef)
      return [...prefix, { text: shape.inner, cls: LINK_CLS, href: field.typeRef.href, mono: true }];
    return [...prefix, { text: shape.inner || "any", cls: MUTED_CLS }];
  }

  if (field.enum && field.enum.length > 0) return literalTokens(field.enum);
  if (field.typeRef) return [{ text: field.type, cls: LINK_CLS, href: field.typeRef.href, mono: true }];
  if (field.typeRefs && field.typeRefs.length > 0) {
    return field.typeRefs.flatMap((r, i) => [
      ...(i > 0 ? [{ text: "or", cls: MUTED_CLS }] : []),
      { text: r.label, cls: LINK_CLS, href: r.href, mono: true },
    ]);
  }

  return [{ text: field.type, cls: MUTED_CLS }];
}

/** Whether a collapsed field should show the `{ … }` "has fields" marker — true
 *  for an object with inline children, false for a union or a leaf. */
export function hasChildPreview(field: ApiFieldView): boolean {
  return !field.union && field.children.length > 0;
}

export interface ConstraintPair {
  name: string;
  value: string;
}

/** Constraints as `name: value` pairs, shown comma-separated with the value in
 *  mono, e.g. `format: int64`, `maxLength: 255`. */
export function constraintPairs(c: ApiConstraint | undefined): ConstraintPair[] {
  if (!c) return [];
  const out: ConstraintPair[] = [];
  if (c.format) out.push({ name: "format", value: c.format });
  if (c.maximum !== undefined) out.push({ name: "maximum", value: String(c.maximum) });
  if (c.minimum !== undefined) out.push({ name: "minimum", value: String(c.minimum) });
  if (c.maxLength !== undefined) out.push({ name: "maxLength", value: String(c.maxLength) });
  if (c.minLength !== undefined) out.push({ name: "minLength", value: String(c.minLength) });
  if (c.pattern) out.push({ name: "pattern", value: c.pattern });
  return out;
}

/** A field opens an expander when it nests object children or union variants. */
export function isExpandable(field: ApiFieldView): boolean {
  return Boolean(field.union) || field.children.length > 0 || field.truncated;
}
