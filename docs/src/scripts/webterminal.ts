/** Load the self-contained terminal tray from a same-origin hashed asset. */
import terminalScriptUrl from "../../_site/customizations/webterminal.js?url";

const SCRIPT_ID = "nimbus-webterminal-script";

function boot(): void {
  if (
    document.getElementById(SCRIPT_ID) ||
    document.getElementById("ch-webterminal-panel")
  )
    return;
  const script = document.createElement("script");
  script.id = SCRIPT_ID;
  script.src = terminalScriptUrl;
  script.async = true;
  document.head.appendChild(script);
}

if (document.readyState === "loading")
  document.addEventListener("DOMContentLoaded", boot, { once: true });
else boot();

// The classic terminal script normally preserves and reattaches its dock during Astro route
// swaps. This is a recovery path for pages entered after a transition where no dock survived.
document.addEventListener("astro:page-load", boot);
