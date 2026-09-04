/**
 * Loads a collapsed sidebar group's children on first expand. The group
 * renders a `<ul data-nb-lazy-src>` placeholder (/nav/<key>/); the fragment at that URL is
 * the same markup SidebarChildren renders at build time. Newly inserted
 * collapsibles are wired with Nimbus's disclosure primitive.
 */
import { makeDisclosure } from "@cloudflare/nimbus-docs/client";

declare global {
  interface Window {
    __chSidebarLazyBound?: boolean;
  }
}

export function bindCollapsibles(root: Element) {
  root.querySelectorAll<HTMLElement>("[data-nb-collapsible]").forEach((el) => {
    if (el.dataset.chBound) return;
    const trigger = Array.from(el.querySelectorAll<HTMLElement>("[data-nb-collapsible-trigger]"))
      .find((candidate) => candidate.closest("[data-nb-collapsible]") === el);
    const content = Array.from(el.querySelectorAll<HTMLElement>("[data-nb-collapsible-content]"))
      .find((candidate) => candidate.closest("[data-nb-collapsible]") === el);
    if (!trigger || !content) return;
    el.dataset.chBound = "";
    makeDisclosure({ trigger, content, defaultOpen: el.dataset.nbDefaultOpen === "true" });
  });
}

async function load(placeholder: HTMLElement) {
  const url = placeholder.getAttribute("data-nb-lazy-src");
  if (!url) return;
  placeholder.removeAttribute("data-nb-lazy-src");
  placeholder.setAttribute("aria-busy", "true");
  try {
    const res = await fetch(url, { headers: { accept: "text/html" } });
    if (!res.ok) throw new Error(`${res.status} ${res.statusText}`);
    const html = await res.text();
    const tpl = document.createElement("template");
    tpl.innerHTML = html.trim();
    const list = tpl.content.querySelector("[data-nb-sidebar-children]") ?? tpl.content.firstElementChild;
    if (!list) throw new Error("empty fragment");
    placeholder.replaceWith(list);
    bindCollapsibles(list);
    loadOpenGroups(list);
  } catch (err) {
    placeholder.setAttribute("data-nb-lazy-src", url);
    placeholder.removeAttribute("aria-busy");
    console.error("[sidebar] could not load navigation fragment", url, err);
  }
}

function loadOpenGroups(root: ParentNode) {
  root.querySelectorAll<HTMLElement>("[data-nb-lazy-src]").forEach((placeholder) => {
    const content = placeholder.closest<HTMLElement>("[data-nb-collapsible-content]");
    if (content?.getAttribute("data-nb-state") === "open") void load(placeholder);
  });
}

if (!window.__chSidebarLazyBound) {
  window.__chSidebarLazyBound = true;
  // Capture phase so the fetch starts before the disclosure toggles open.
  document.addEventListener(
    "click",
    (event) => {
      const trigger = (event.target as Element | null)?.closest("[data-nb-collapsible-trigger]");
      if (!trigger) return;
      const group = trigger.closest("[data-nb-sidebar-group]");
      const content = group?.querySelector(":scope > [data-nb-collapsible-content]");
      const placeholder = content?.querySelector<HTMLElement>("[data-nb-lazy-src]");
      if (placeholder) void load(placeholder);
    },
    true,
  );

  // Restored sidebar state can open a lazy group without producing a click.
  // Populate those panels immediately so an open group never shows only its
  // placeholder after a route swap or reload.
  const loadRestoredGroups = () => requestAnimationFrame(() => loadOpenGroups(document));
  document.addEventListener("astro:page-load", loadRestoredGroups);
  loadRestoredGroups();
}
