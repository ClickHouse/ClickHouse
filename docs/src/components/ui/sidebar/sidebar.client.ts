/** Sidebar runtime: filter, persistence, "/" shortcut. */

import { mount } from "@cloudflare/nimbus-docs/client";

const STORAGE_KEY = "sidebar-state";

interface SidebarState {
  hash: string;
  open: Record<string, boolean>;
  scroll: number;
}

function ownedTrigger(group: HTMLElement): HTMLElement | undefined {
  return Array.from(group.querySelectorAll<HTMLElement>("[data-nb-collapsible-trigger]"))
    .find((candidate) => candidate.closest("[data-nb-sidebar-group]") === group);
}

function ownedLabel(group: HTMLElement): HTMLElement | undefined {
  return Array.from(group.querySelectorAll<HTMLElement>("[data-nb-sidebar-group-label]"))
    .find((candidate) => candidate.closest("[data-nb-sidebar-group]") === group);
}

function groupKey(group: HTMLElement): string {
  const labels: string[] = [];
  let current: HTMLElement | null = group;
  while (current) {
    const label = ownedLabel(current)?.textContent?.trim().replace(/\s+/g, " ") ?? "";
    labels.unshift(label);
    current = current.parentElement?.closest<HTMLElement>("[data-nb-sidebar-group]") ?? null;
  }
  return labels.join("\u001f");
}

function initSidebar(root: HTMLElement): () => void {
  const teardowns: Array<() => void> = [];
  const persist = root.hasAttribute("data-nb-sidebar-persist");

  const filterTeardown = initFilter(root);
  if (filterTeardown) teardowns.push(filterTeardown);

  if (persist) {
    const persistTeardown = initPersistence(root);
    if (persistTeardown) teardowns.push(persistTeardown);
  }

  return () => teardowns.forEach((t) => t());
}

// ---------------------------------------------------------------------------
// Filter
// ---------------------------------------------------------------------------

function initFilter(root: HTMLElement): (() => void) | null {
  const input = root.querySelector<HTMLInputElement>("[data-nb-sidebar-filter-input]");
  // SidebarFilter sits above the scroll container that owns Sidebar. Resolve
  // it from the enclosing navigation region so the desktop rail and the
  // cloned mobile drawer share the same binding path.
  const inputElement =
    input ?? root.closest("nav")?.querySelector<HTMLInputElement>("[data-nb-sidebar-filter-input]") ?? null;
  if (!inputElement) return null;

  function handleInput() {
    const query = inputElement!.value.trim().toLowerCase();
    if (!query) {
      resetFilter(root);
      return;
    }
    applyFilter(root, query);
  }

  function handleKeydown(e: KeyboardEvent) {
    if (e.key === "Escape") {
      inputElement!.value = "";
      handleInput();
      inputElement!.blur();
    }
  }

  inputElement.addEventListener("input", handleInput);
  inputElement.addEventListener("keydown", handleKeydown);

  return () => {
    inputElement.removeEventListener("input", handleInput);
    inputElement.removeEventListener("keydown", handleKeydown);
    resetFilter(root);
  };
}

function resetFilter(root: HTMLElement): void {
  root.querySelectorAll<HTMLElement>("[data-nb-sidebar-hidden]").forEach((el) => {
    el.removeAttribute("data-nb-sidebar-hidden");
  });
  // Reset groups opened by the filter back to their saved state.
  root
    .querySelectorAll<HTMLElement>("[data-nb-sidebar-group][data-nb-opened-by-filter]")
    .forEach((group) => {
      const trigger = ownedTrigger(group);
      trigger?.click();
      group.removeAttribute("data-nb-opened-by-filter");
    });
}

function applyFilter(root: HTMLElement, query: string): void {
  const links = root.querySelectorAll<HTMLElement>("[data-nb-sidebar-link]");
  const groups = root.querySelectorAll<HTMLElement>("[data-nb-sidebar-group]");

  links.forEach((link) => link.setAttribute("data-nb-sidebar-hidden", ""));
  groups.forEach((group) => group.setAttribute("data-nb-sidebar-hidden", ""));

  links.forEach((link) => {
    const text = link.textContent?.toLowerCase() ?? "";
    if (!text.includes(query)) return;
    link.removeAttribute("data-nb-sidebar-hidden");
    revealAncestors(link, root);
  });

  groups.forEach((group) => {
    const label = group.querySelector("[data-nb-sidebar-group-label]");
    const text = label?.textContent?.toLowerCase() ?? "";
    if (!text.includes(query)) return;
    group.removeAttribute("data-nb-sidebar-hidden");
    openGroup(group);
    group.querySelectorAll<HTMLElement>("[data-nb-sidebar-link], [data-nb-sidebar-group]")
      .forEach((child) => child.removeAttribute("data-nb-sidebar-hidden"));
  });
}

function revealAncestors(el: HTMLElement, scope: Element): void {
  let parent: HTMLElement | null = el.parentElement;
  while (parent && parent !== scope) {
    if (parent.hasAttribute("data-nb-sidebar-group")) {
      parent.removeAttribute("data-nb-sidebar-hidden");
      openGroup(parent);
    }
    parent = parent.parentElement;
  }
}

function openGroup(group: HTMLElement): void {
  const trigger = ownedTrigger(group);
  if (!trigger) return;
  if (trigger.getAttribute("data-nb-state") === "open") return;
  group.setAttribute("data-nb-opened-by-filter", "");
  trigger.click();
}

// ---------------------------------------------------------------------------
// Persistence (open state + scroll)
// ---------------------------------------------------------------------------

function initPersistence(root: HTMLElement): (() => void) | null {
  // The fixed <aside> clips overflow; the nested navigation pane owns the
  // actual scrollbar. Persist that pane so its position survives route swaps.
  const scrollHost: HTMLElement =
    root.closest<HTMLElement>(".ch-sidebar-scroll") ??
    root.closest<HTMLElement>("aside") ??
    root;
  const hash = root.dataset.nbSidebarHash ?? "";

  function readState(): SidebarState {
    const groups = root.querySelectorAll<HTMLElement>("[data-nb-sidebar-group]");
    const open: Record<string, boolean> = {};
    groups.forEach((group) => {
      const trigger = ownedTrigger(group);
      // Fixed section headings have no disclosure trigger and always remain
      // open. Recording them as open also repairs state written by older code.
      open[groupKey(group)] = !trigger || trigger.getAttribute("data-nb-state") === "open";
    });
    return { hash, open, scroll: scrollHost.scrollTop };
  }

  function save() {
    try {
      sessionStorage.setItem(STORAGE_KEY, JSON.stringify(readState()));
    } catch {}
  }

  // Observe the whole tree so disclosures inserted by lazy navigation
  // fragments are persisted too.
  const observer = new MutationObserver(save);
  observer.observe(root, {
    subtree: true,
    attributes: true,
    attributeFilter: ["data-nb-state"],
  });

  function handleVisibility() {
    if (document.visibilityState === "hidden") save();
  }
  document.addEventListener("visibilitychange", handleVisibility);
  document.addEventListener("astro:before-swap", save);
  window.addEventListener("pagehide", save);

  let raf = 0;
  function handleScroll() {
    cancelAnimationFrame(raf);
    raf = requestAnimationFrame(save);
  }
  scrollHost.addEventListener("scroll", handleScroll);

  return () => {
    observer.disconnect();
    document.removeEventListener("visibilitychange", handleVisibility);
    document.removeEventListener("astro:before-swap", save);
    window.removeEventListener("pagehide", save);
    scrollHost.removeEventListener("scroll", handleScroll);
    cancelAnimationFrame(raf);
  };
}

// ---------------------------------------------------------------------------
// Global `/` shortcut — bound once at module load
// ---------------------------------------------------------------------------

(function bindFilterShortcut() {
  if (document.documentElement.hasAttribute("data-nb-sidebar-shortcut-bound")) return;
  document.documentElement.setAttribute("data-nb-sidebar-shortcut-bound", "");

  document.addEventListener("keydown", (e) => {
    if (e.key !== "/") return;
    const active = document.activeElement as HTMLElement | null;
    if (
      active &&
      (active.tagName === "INPUT" ||
        active.tagName === "TEXTAREA" ||
        active.isContentEditable)
    ) {
      return;
    }
    const desktopInput = document.querySelector<HTMLInputElement>(
      "[data-nb-sidebar-persist] ~ * [data-nb-sidebar-filter-input], [data-nb-desktop-sidebar] [data-nb-sidebar-filter-input]",
    );
    if (!desktopInput) return;
    e.preventDefault();
    desktopInput.focus();
  });
})();

mount("[data-nb-sidebar]", initSidebar);
