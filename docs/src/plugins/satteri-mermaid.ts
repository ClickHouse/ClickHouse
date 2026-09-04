import type { HastPluginDefinition } from "satteri";

/**
 * Turns ```mermaid fences into `<pre class="mermaid">` blocks for the lazy
 * client renderer (src/scripts/mermaid.ts). Requires `mermaid` to be listed
 * in Astro's `markdown.syntaxHighlight.excludeLangs` so Shiki leaves the
 * fence as `<pre><code class="language-mermaid">`.
 */
export function mermaidBlocks(): HastPluginDefinition {
  return {
    name: "clickhouse:mermaid-blocks",
    element: {
      filter: ["pre"],
      visit(node, ctx) {
        const code = node.children?.find(
          (c): c is Extract<typeof c, { type: "element" }> => c.type === "element" && c.tagName === "code",
        );
        if (!code) return;
        const classes = code.properties?.className;
        const list = Array.isArray(classes) ? classes.map(String) : typeof classes === "string" ? [classes] : [];
        if (!list.includes("language-mermaid")) return;
        return {
          type: "element",
          tagName: "pre",
          properties: { className: ["mermaid"], "data-mermaid": "" },
          children: [{ type: "text", value: ctx.textContent(code) }],
        };
      },
    },
  };
}
