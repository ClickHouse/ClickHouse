/** Sets the platform-correct shortcut hint on the search trigger (⌘ on macOS, Ctrl elsewhere). */

import { mount } from "@cloudflare/nimbus-docs/client";

mount("[data-search-trigger]", (btn) => {
  const nav = navigator as Navigator & { userAgentData?: { platform?: string } };
  const platform = nav.userAgentData?.platform ?? "";
  const isMac = platform
    ? /mac/i.test(platform)
    : /mac|iphone|ipod|ipad/i.test(navigator.userAgent);
  if (isMac) {
    btn.setAttribute("aria-keyshortcuts", "Meta+K");
    const key = btn.querySelector("[data-shortcut-key]");
    if (key) key.textContent = "⌘";
  }
  return () => {};
});
