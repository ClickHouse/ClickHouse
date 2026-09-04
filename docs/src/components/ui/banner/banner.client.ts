/**
 * Storage key: `nb-banner-dismissed-{id}`. Value is "0" for permanent,
 * or a future timestamp (ms) for time-limited dismissal.
 */

import { mount } from "@cloudflare/nimbus-docs/client";

const KEY_PREFIX = "nb-banner-dismissed-";

function initBanner(banner: HTMLElement): () => void {
  const id = banner.dataset.nbBannerDismiss;
  if (!id) return () => {};

  const key = `${KEY_PREFIX}${id}`;

  try {
    const stored = localStorage.getItem(key);
    if (stored) {
      const expiry = Number(stored);
      if (expiry === 0 || expiry > Date.now()) {
        banner.remove();
        return () => {};
      }
      localStorage.removeItem(key);
    }
  } catch {
    // localStorage unavailable; show without persistence.
  }

  const btn = banner.querySelector<HTMLButtonElement>("[data-nb-banner-close]");
  if (!btn) return () => {};

  function handleClick() {
    const days = Number(banner.dataset.nbBannerDays) || 0;
    const value = days > 0 ? String(Date.now() + days * 86400000) : "0";
    try {
      localStorage.setItem(key, value);
    } catch {
      // localStorage unavailable; dismissal is session-only.
    }
    banner.remove();
  }

  btn.addEventListener("click", handleClick);

  return () => btn.removeEventListener("click", handleClick);
}

mount("[data-nb-banner-dismiss]", initBanner);
