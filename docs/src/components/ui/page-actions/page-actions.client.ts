import { mount } from "@cloudflare/nimbus-docs/client";

function initPageActions(root: HTMLElement): () => void {
  const copyBtn = root.querySelector<HTMLButtonElement>("[data-nb-page-actions-copy]");
  const copyIcon = root.querySelector<SVGElement>("[data-nb-page-actions-copy-icon]");
  const checkIcon = root.querySelector<SVGElement>("[data-nb-page-actions-check-icon]");
  const label = root.querySelector<HTMLSpanElement>("[data-nb-page-actions-label]");
  const mdUrl = root.dataset.mdUrl;

  if (!copyBtn || !mdUrl) return () => {};

  let resetTimer: number | undefined;

  function showState(state: "copied" | "error") {
    if (!copyIcon || !checkIcon || !label) return;
    if (state === "copied") {
      copyIcon.classList.add("hidden");
      checkIcon.classList.remove("hidden");
      label.textContent = "Copied";
    } else {
      label.textContent = "Couldn't copy";
    }
    if (resetTimer) window.clearTimeout(resetTimer);
    resetTimer = window.setTimeout(() => {
      copyIcon.classList.remove("hidden");
      checkIcon.classList.add("hidden");
      label.textContent = "Copy page";
    }, 1500);
  }

  async function handleCopyPage() {
    try {
      const res = await fetch(mdUrl!);
      if (!res.ok) {
        showState("error");
        return;
      }
      const text = await res.text();
      await navigator.clipboard.writeText(text);
      showState("copied");
    } catch {
      showState("error");
    }
  }

  copyBtn.addEventListener("click", handleCopyPage);

  return () => {
    if (resetTimer) window.clearTimeout(resetTimer);
    copyBtn.removeEventListener("click", handleCopyPage);
  };
}

mount("[data-nb-page-actions]", initPageActions);
