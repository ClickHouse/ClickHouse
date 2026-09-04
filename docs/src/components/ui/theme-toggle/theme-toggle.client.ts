/**
 * theme-toggle.client.ts — system/light/dark preference. Writes to localStorage
 * ("ui-mode"); BaseLayout's pre-paint script owns DOM application so view
 * transitions, OS changes, and cross-tab edits stay in sync.
 */

import { mount } from "@cloudflare/nimbus-docs/client";

declare global {
  interface Window {
    __nbApplyTheme?: () => void;
  }
}

function initThemeToggle(root: HTMLElement): () => void {
  const controller = new AbortController();
  root.querySelectorAll<HTMLButtonElement>("[data-theme-choice]").forEach((button) => {
    button.addEventListener("click", () => {
      const choice = button.dataset.themeChoice;
      if (choice !== "system" && choice !== "light" && choice !== "dark") return;
      try {
        localStorage.setItem("ui-mode", choice);
      } catch {
        // A restricted storage context should not prevent an in-page theme change.
      }
      window.__nbApplyTheme?.();
    }, { signal: controller.signal });
  });

  try {
    const choice = localStorage.getItem("ui-mode") ?? "system";
    root.querySelectorAll<HTMLButtonElement>("[data-theme-choice]").forEach((button) => {
      button.setAttribute("aria-pressed", String(button.dataset.themeChoice === choice));
    });
  } catch {
    root.querySelector<HTMLButtonElement>('[data-theme-choice="system"]')?.setAttribute("aria-pressed", "true");
  }

  return () => controller.abort();
}

mount("[data-nb-theme-preference]", initThemeToggle);
