/**
 * Renders `<pre class="mermaid">` diagrams on demand. The mermaid bundle is
 * large, so it is only imported on pages that contain a diagram, and only once.
 */
let mermaidPromise: Promise<typeof import("mermaid").default> | null = null;

async function renderAll() {
  const blocks = Array.from(document.querySelectorAll<HTMLElement>("pre.mermaid:not([data-processed])"));
  if (blocks.length === 0) return;
  mermaidPromise ??= import("mermaid").then((m) => m.default);
  const mermaid = await mermaidPromise;
  const dark = document.documentElement.getAttribute("data-mode") === "dark";
  mermaid.initialize({ startOnLoad: false, theme: dark ? "dark" : "default", securityLevel: "strict" });
  await mermaid.run({ nodes: blocks });
}

renderAll();
document.addEventListener("astro:page-load", renderAll);
