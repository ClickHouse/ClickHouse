import { mount } from "@cloudflare/nimbus-docs/client";
import type {
  SearchProvider,
  SearchResult,
} from "@cloudflare/nimbus-docs/types";
import { provider } from "./providers/pagefind";

export interface SearchConfig {
  input: HTMLInputElement;
  resultsContainer: HTMLElement;
  emptyState: HTMLElement;
  provider: SearchProvider;
  onNavigate?: () => void;
}

export interface SearchInstance {
  reset(): Promise<void>;
  destroy(): void;
}

export function initSearch(config: SearchConfig): SearchInstance {
  const { input, resultsContainer, emptyState, provider, onNavigate } = config;

  let initialized = false;
  let activeIndex = -1;
  let resultIdCounter = 0;
  let debounceTimer: ReturnType<typeof setTimeout> | undefined;
  let activeController: AbortController | undefined;

  function getOptions(): HTMLElement[] {
    return Array.from(
      resultsContainer.querySelectorAll<HTMLElement>("[role='option']"),
    );
  }

  function updateActive(newIndex: number): void {
    const options = getOptions();
    if (options.length === 0) {
      activeIndex = -1;
      input.removeAttribute("aria-activedescendant");
      return;
    }
    activeIndex = Math.max(-1, Math.min(newIndex, options.length - 1));
    options.forEach((option, index) => {
      if (index === activeIndex) {
        option.setAttribute("data-highlighted", "");
        option.scrollIntoView({ block: "nearest" });
        input.setAttribute("aria-activedescendant", option.id);
      } else {
        option.removeAttribute("data-highlighted");
      }
    });
    if (activeIndex < 0) input.removeAttribute("aria-activedescendant");
  }

  function clearResults(): void {
    for (const result of resultsContainer.querySelectorAll("[role='option']"))
      result.remove();
    input.setAttribute("aria-expanded", "false");
    input.removeAttribute("aria-activedescendant");
  }

  function resultLink(
    title: string,
    href: string,
    className: string,
  ): HTMLAnchorElement {
    const link = document.createElement("a");
    link.href = href;
    link.className = className;
    link.textContent = title;
    link.addEventListener("click", () => onNavigate?.());
    return link;
  }

  function buildResult(result: SearchResult): HTMLElement {
    const option = document.createElement("div");
    option.id = `search-result-${resultIdCounter++}`;
    option.setAttribute("role", "option");
    option.className =
      "rounded-lg px-2 py-2 transition-colors cursor-pointer hover:bg-accent focus-within:bg-accent data-[highlighted]:bg-accent";

    const link = resultLink(
      result.title,
      result.url,
      "block truncate text-sm font-medium text-foreground no-underline focus-visible:outline-none",
    );
    option.appendChild(link);

    if (result.snippet) {
      const snippet = document.createElement("p");
      snippet.className =
        "mt-1 line-clamp-2 text-xs leading-relaxed text-muted-foreground";
      snippet.innerHTML = result.snippet;
      option.appendChild(snippet);
    }

    if (result.subResults?.length) {
      const subList = document.createElement("div");
      subList.className = "mt-2 border-l border-border pl-3";
      for (const sub of result.subResults.slice(0, 3)) {
        subList.appendChild(
          resultLink(
            sub.title,
            sub.url,
            "block truncate py-0.5 text-xs text-muted-foreground no-underline hover:text-foreground",
          ),
        );
      }
      option.appendChild(subList);
    }

    option.addEventListener("click", (event) => {
      if ((event.target as Element | null)?.closest("a")) return;
      link.click();
    });

    return option;
  }

  async function ensureInitialized(): Promise<boolean> {
    if (initialized) return true;
    try {
      await provider.init?.();
      initialized = true;
      return true;
    } catch {
      emptyState.textContent = "Search is available after a production build.";
      return false;
    }
  }

  async function runSearch(query: string): Promise<void> {
    activeController?.abort();
    activeController = new AbortController();
    const signal = activeController.signal;

    emptyState.style.display = "";
    emptyState.textContent = "Searching…";
    clearResults();

    if (!(await ensureInitialized()) || signal.aborted) return;

    try {
      const results = await provider.search(query, { signal });
      if (signal.aborted) return;

      clearResults();
      activeIndex = -1;

      if (results.length === 0) {
        emptyState.style.display = "";
        emptyState.textContent = "No results found.";
        return;
      }

      emptyState.style.display = "none";
      input.setAttribute("aria-expanded", "true");
      for (const result of results)
        resultsContainer.appendChild(buildResult(result));
    } catch {
      if (signal.aborted) return;
      clearResults();
      emptyState.style.display = "";
      emptyState.textContent = "Search is temporarily unavailable.";
    }
  }

  function handleInput(): void {
    if (debounceTimer) clearTimeout(debounceTimer);
    debounceTimer = setTimeout(() => {
      const query = input.value.trim();
      if (!query) {
        activeController?.abort();
        clearResults();
        emptyState.style.display = "";
        emptyState.textContent = "Type to search…";
        return;
      }
      void runSearch(query);
    }, 150);
  }

  function handleKeydown(event: KeyboardEvent): void {
    const options = getOptions();
    if (event.key === "ArrowDown") {
      event.preventDefault();
      updateActive(activeIndex + 1);
    } else if (event.key === "ArrowUp") {
      event.preventDefault();
      updateActive(activeIndex - 1);
    } else if (event.key === "Home") {
      event.preventDefault();
      updateActive(0);
    } else if (event.key === "End") {
      event.preventDefault();
      updateActive(options.length - 1);
    } else if (event.key === "Enter" && activeIndex >= 0) {
      event.preventDefault();
      options[activeIndex]?.querySelector<HTMLAnchorElement>("a")?.click();
    }
  }

  input.addEventListener("input", handleInput);
  input.closest("dialog")?.addEventListener("keydown", handleKeydown);

  return {
    async reset() {
      activeController?.abort();
      if (debounceTimer) clearTimeout(debounceTimer);
      input.value = "";
      input.focus();
      activeIndex = -1;
      clearResults();
      emptyState.style.display = "";
      emptyState.textContent = "Type to search…";
      await ensureInitialized();
    },
    destroy() {
      activeController?.abort();
      if (debounceTimer) clearTimeout(debounceTimer);
      input.removeEventListener("input", handleInput);
      input.closest("dialog")?.removeEventListener("keydown", handleKeydown);
    },
  };
}

// ---------------------------------------------------------------------------
// Bootstrap — imported for its side effects by SearchDialog.astro
// (`import "./search.client"`). Wires each dialog through mount() and binds the
// global open shortcut once.
// ---------------------------------------------------------------------------

type SearchDialogElement = HTMLDialogElement & {
  __openSearchDialog?: () => void;
};

function primaryDialog(): SearchDialogElement | null {
  return document.querySelector<SearchDialogElement>(
    "[data-search-dialog][data-search-ready]",
  );
}

// The open shortcut and trigger delegation live on `document`, which survives
// view transitions, so they are bound once for the page's lifetime — never
// through mount()'s per-element setup/teardown. A module-scoped boolean (not an
// <html> attribute) is the guard: ClientRouter resets <html> on every swap but
// keeps document listeners, so an attribute guard would stack a duplicate
// keydown handler each navigation (Cmd+K then toggles twice).
let globalsBound = false;

function bindGlobals() {
  if (globalsBound) return;
  globalsBound = true;

  document.addEventListener("click", (event) => {
    if (event.defaultPrevented) return;
    const trigger = (event.target as Element | null)?.closest(
      "[data-search-trigger]",
    );
    if (!trigger) return;
    primaryDialog()?.__openSearchDialog?.();
  });

  document.addEventListener("keydown", (event) => {
    if (event.defaultPrevented) return;
    if (!(event.metaKey || event.ctrlKey) || event.key.toLowerCase() !== "k")
      return;
    const dialog = primaryDialog();
    if (!dialog) return;
    event.preventDefault();
    if (dialog.open) dialog.close();
    else dialog.__openSearchDialog?.();
  });
}

// Per-element wiring: idempotent discovery now and on astro:page-load, teardown
// on astro:before-swap. Replaces the hand-rolled data-search-ready init loop;
// data-search-ready is now just the "wired" marker primaryDialog() selects on.
mount("[data-search-dialog]", (root) => {
  const dialog = root as SearchDialogElement;
  dialog.setAttribute("data-search-ready", "true");

  const input = dialog.querySelector<HTMLInputElement>("[data-search-input]");
  const resultsContainer = dialog.querySelector<HTMLElement>(
    "[data-search-results]",
  );
  const emptyState = dialog.querySelector<HTMLElement>("[data-search-empty]");
  if (!input || !resultsContainer || !emptyState) return () => {};

  const search = initSearch({
    input,
    resultsContainer,
    emptyState,
    provider,
    onNavigate: () => dialog.close(),
  });

  dialog.__openSearchDialog = () => {
    if (!dialog.open) dialog.showModal();
    void search.reset();
  };

  return () => search.destroy();
});

bindGlobals();
