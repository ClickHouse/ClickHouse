/** Wires <Tabs>; auto-detects manual triggers vs. synthesized-from-TabItem mode. */

import { mount, initTabs } from "@cloudflare/nimbus-docs/client";

const TRIGGER_CLASS =
  "inline-flex shrink-0 cursor-pointer items-center gap-2 px-4 py-2 text-sm font-medium leading-6 whitespace-nowrap text-muted-foreground transition-colors hover:text-foreground aria-selected:text-primary focus-visible:rounded-sm focus-visible:outline-2 focus-visible:outline-ring focus-visible:outline-offset-[-2px]";

let counter = 0;

function initTabContainer(container: HTMLElement): () => void {
  const id = `nb-tabs-${counter++}`;
  const syncKey = container.dataset.nbSyncKey;
  const tablist = container.querySelector<HTMLElement>("[role=tablist]");
  const indicator = container.querySelector<HTMLElement>("[data-nb-tabs-indicator]");

  // Scope to this container so a nested <Tabs>'s triggers don't flip the
  // parent into manual mode (or vice-versa), independent of mount order.
  const existingTriggers = Array.from(
    container.querySelectorAll("[data-nb-tabs-trigger]"),
  ).filter((t) => (t as HTMLElement).closest("[data-nb-tabs]") === container);
  const synthesize = existingTriggers.length === 0;

  if (synthesize && tablist) {
    // Only this container's own panels — exclude a nested <Tabs>'s panels,
    // whose nearest [data-nb-tabs] ancestor is the inner container.
    const panels = Array.from(
      container.querySelectorAll<HTMLElement>("[data-nb-tabs-content]"),
    ).filter((p) => p.closest("[data-nb-tabs]") === container);

    panels.forEach((panel, i) => {
      const label = panel.dataset.nbTabLabel ?? "Tab";
      const btn = document.createElement("button");
      btn.role = "tab";
      btn.type = "button";
      btn.className = TRIGGER_CLASS;
      const icon = panel.dataset.nbTabIcon;
      if (icon) {
        const image = document.createElement("img");
        image.src = icon;
        image.alt = "";
        image.setAttribute("aria-hidden", "true");
        image.className = "h-4 w-4 shrink-0 object-contain";
        btn.append(image);
      }
      btn.append(document.createTextNode(label));
      btn.setAttribute("data-nb-tabs-trigger", "");

      const panelId = `${id}-panel-${i}`;
      const tabId = `${id}-tab-${i}`;
      btn.id = tabId;
      btn.setAttribute("aria-controls", panelId);
      panel.id = panelId;
      panel.setAttribute("aria-labelledby", tabId);

      if (indicator) {
        tablist.insertBefore(btn, indicator);
      } else {
        tablist.appendChild(btn);
      }
    });
  }

  const instance = initTabs({
    container,
    tabSelector: "[data-nb-tabs-trigger]",
    panelSelector: "[data-nb-tabs-content]",
    boundarySelector: "[data-nb-tabs]",
    indicator,
    sync: syncKey ? { key: `ui-synced-tabs__${syncKey}` } : undefined,
    // Keep the active tab within the horizontally-scrollable (scrollbar-hidden)
    // strip's visible range. Fires on every activate() — including the initial
    // paint and a synced/restored selection — so a right-edge active tab can't
    // render off-screen with no affordance. scrollLeft directly (not
    // scrollIntoView, which would also scroll the page vertically).
    onActivate: (index) => {
      if (!tablist) return;
      const trigger =
        tablist.querySelectorAll<HTMLElement>("[data-nb-tabs-trigger]")[index];
      if (!trigger) return;
      const left = trigger.offsetLeft;
      const right = left + trigger.offsetWidth;
      if (left < tablist.scrollLeft) {
        tablist.scrollLeft = left;
      } else if (right > tablist.scrollLeft + tablist.clientWidth) {
        tablist.scrollLeft = right - tablist.clientWidth;
      }
    },
  });

  // Cross-instance sync is keyed by trigger label; duplicate labels in a group
  // resolve by first-match and activate the wrong panel. Surface it in dev.
  if (import.meta.env.DEV && syncKey && tablist) {
    const labels = Array.from(
      tablist.querySelectorAll<HTMLElement>("[data-nb-tabs-trigger]"),
    )
      .filter((t) => t.closest("[data-nb-tabs]") === container)
      .map((t) => (t.textContent ?? "").trim());
    const dupes = [...new Set(labels.filter((l, i) => labels.indexOf(l) !== i))];
    if (dupes.length) {
      console.warn(
        `[nimbus] <Tabs syncKey="${syncKey}"> has duplicate tab labels (${dupes
          .map((d) => `"${d}"`)
          .join(", ")}). Sync is keyed by label, so a duplicate activates the ` +
          `first match. Give each tab a unique label.`,
      );
    }
  }

  return () => {
    instance.destroy();
    // Remove synthesized triggers so re-mount doesn't double up.
    if (synthesize && tablist) {
      tablist.querySelectorAll("[data-nb-tabs-trigger]").forEach((b) => b.remove());
    }
  };
}

mount("[data-nb-tabs]", initTabContainer);
