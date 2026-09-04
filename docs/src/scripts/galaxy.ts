/** Load the Galaxy event transport from the generated, same-origin asset. */
import galaxyScriptUrl from "@/generated/galaxy.js?url";

const SCRIPT_ID = "nimbus-galaxy-script";

function boot(): void {
  if (document.getElementById(SCRIPT_ID)) return;
  const script = document.createElement("script");
  script.id = SCRIPT_ID;
  script.src = galaxyScriptUrl;
  script.async = true;
  document.head.appendChild(script);
}

if (document.readyState === "loading")
  document.addEventListener("DOMContentLoaded", boot, { once: true });
else boot();
