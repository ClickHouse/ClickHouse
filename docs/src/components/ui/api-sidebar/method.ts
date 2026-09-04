export type MethodVariant = "get" | "post" | "put" | "delete" | "other";

// Fold an HTTP method onto its `--nb-m-*` palette key; "" for a missing method.
export function methodVariant(method: string | undefined): MethodVariant | "" {
  switch (method?.toLowerCase()) {
    case "get":
    case "head":
      return "get";
    case "post":
      return "post";
    case "put":
    case "patch":
      return "put";
    case "delete":
      return "delete";
    default:
      return method ? "other" : "";
  }
}
