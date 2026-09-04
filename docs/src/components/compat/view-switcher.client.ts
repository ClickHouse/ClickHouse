import { mount } from "@cloudflare/nimbus-docs/client";

const STORAGE_KEY = "clickhouse-docs-view";
const VIEW_CHANGE_EVENT = "ch:view-change";

function pageViews(): HTMLElement[] {
  return Array.from(document.querySelectorAll<HTMLElement>(".docs-content .ch-view[data-view]"));
}

function viewTitles(views: HTMLElement[]): string[] {
  return [...new Set(views.map((view) => view.dataset.view).filter((title): title is string => Boolean(title)))];
}

function selectedTitle(titles: string[]): string {
  const stored = sessionStorage.getItem(STORAGE_KEY);
  return stored && titles.includes(stored) ? stored : titles[0];
}

function updateToc(views: HTMLElement[], activeTitle: string) {
  const activeView = views.find((view) => view.dataset.view === activeTitle);
  if (!activeView) return;

  const headings = Array.from(document.querySelectorAll<HTMLElement>(".docs-content h2[id], .docs-content h3[id]"))
    .filter((heading) => {
      const owner = heading.closest<HTMLElement>(".ch-view");
      return owner === null || owner === activeView;
    });

  const label = (heading: HTMLElement) => {
    const copy = heading.cloneNode(true) as HTMLElement;
    copy.querySelectorAll(".heading-anchor").forEach((anchor) => anchor.remove());
    return copy.textContent?.trim() || heading.id;
  };

  document.querySelectorAll<HTMLElement>("[data-nb-toc] nav > ul").forEach((list) => {
    list.replaceChildren(...headings.map((heading) => {
      const depth = heading.tagName === "H3" ? 1 : 0;
      const item = document.createElement("li");
      item.dataset.nbTocDepth = String(depth);
      const link = document.createElement("a");
      link.href = `#${heading.id}`;
      link.dataset.nbTocLink = "";
      link.dataset.nbSlug = heading.id;
      link.className = "no-underline";
      link.style.setProperty("--ch-toc-depth", String(depth));
      link.textContent = label(heading);
      item.append(link);
      return item;
    }));
  });

  document.querySelectorAll<HTMLSelectElement>("[data-nb-mobile-toc-select]").forEach((select) => {
    const overview = document.createElement("option");
    overview.value = "_top";
    overview.textContent = "Overview";
    select.replaceChildren(overview, ...headings.map((heading) => {
      const option = document.createElement("option");
      option.value = heading.id;
      option.textContent = `${heading.tagName === "H3" ? " " : ""}${label(heading)}`;
      return option;
    }));
  });
}

function applyView(activeTitle: string) {
  const views = pageViews();
  for (const view of views) {
    const active = view.dataset.view === activeTitle;
    view.hidden = !active;
    view.dataset.chViewActive = String(active);
  }

  document.querySelectorAll<HTMLElement>("[data-ch-view-switcher]").forEach((switcher) => {
    const label = switcher.querySelector<HTMLElement>("[data-ch-view-switcher-label]");
    if (label) label.textContent = activeTitle;
    const trigger = switcher.querySelector<HTMLElement>(".ch-view-switcher-trigger");
    trigger?.setAttribute("aria-label", `Switch view: ${activeTitle}`);
    switcher.querySelectorAll<HTMLElement>("[role=menuitemradio]").forEach((item) => {
      const active = item.dataset.viewOption === activeTitle;
      item.setAttribute("aria-checked", String(active));
      item.toggleAttribute("data-selected", active);
    });
  });

  updateToc(views, activeTitle);
  window.dispatchEvent(new CustomEvent(VIEW_CHANGE_EVENT));
}

function initViewSwitcher(root: HTMLElement): () => void {
  const views = pageViews();
  const titles = viewTitles(views);
  const trigger = root.querySelector<HTMLButtonElement>(".ch-view-switcher-trigger");
  const menu = root.querySelector<HTMLElement>("[data-ch-view-switcher-menu]");
  if (titles.length < 2 || !trigger || !menu) return () => {};

  root.hidden = false;
  menu.replaceChildren();

  for (const title of titles) {
    const option = document.createElement("button");
    option.type = "button";
    option.className = "ch-view-switcher-option";
    option.dataset.viewOption = title;
    option.setAttribute("role", "menuitemradio");
    option.innerHTML = `<span></span><svg aria-hidden="true" viewBox="0 0 16 16" fill="none"><path d="m3.5 8 3 3 6-6" /></svg>`;
    option.querySelector("span")!.textContent = title;
    menu.append(option);
  }

  const controller = new AbortController();
  const { signal } = controller;

  const close = (focusTrigger = false) => {
    root.removeAttribute("data-open");
    trigger.setAttribute("aria-expanded", "false");
    menu.hidden = true;
    if (focusTrigger) trigger.focus();
  };

  const open = () => {
    root.setAttribute("data-open", "");
    trigger.setAttribute("aria-expanded", "true");
    menu.hidden = false;
    menu.querySelector<HTMLElement>("[data-selected]")?.focus();
  };

  trigger.addEventListener("click", () => root.hasAttribute("data-open") ? close() : open(), { signal });
  menu.addEventListener("click", (event) => {
    const option = (event.target as Element).closest<HTMLElement>("[data-view-option]");
    const title = option?.dataset.viewOption;
    if (!title) return;
    sessionStorage.setItem(STORAGE_KEY, title);
    applyView(title);
    close(true);
  }, { signal });
  menu.addEventListener("keydown", (event) => {
    const options = Array.from(menu.querySelectorAll<HTMLElement>("[data-view-option]"));
    const index = options.indexOf(document.activeElement as HTMLElement);
    if (event.key === "Escape") {
      event.preventDefault();
      close(true);
    } else if (event.key === "ArrowDown" || event.key === "ArrowUp") {
      event.preventDefault();
      const offset = event.key === "ArrowDown" ? 1 : -1;
      options[(index + offset + options.length) % options.length]?.focus();
    }
  }, { signal });
  document.addEventListener("click", (event) => {
    if (!root.contains(event.target as Node)) close();
  }, { signal });

  applyView(selectedTitle(titles));
  return () => controller.abort();
}

mount("[data-ch-view-switcher]", initViewSwitcher);
