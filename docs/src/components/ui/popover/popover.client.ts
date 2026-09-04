import { mount } from "@cloudflare/nimbus-docs/client";

type Placement = "bottom-start" | "bottom-end" | "top-start" | "top-end";

const GAP = 4;
const VIEWPORT_PADDING = 8;

function anchor(trigger: HTMLElement, content: HTMLElement) {
  const placement = (content.dataset.placement as Placement | undefined) ?? "bottom-start";
  const triggerRect = trigger.getBoundingClientRect();
  const contentRect = content.getBoundingClientRect();
  const vw = window.innerWidth;
  const vh = window.innerHeight;

  let top: number;
  if (placement.startsWith("bottom")) {
    top = triggerRect.bottom + GAP;
    if (top + contentRect.height > vh - VIEWPORT_PADDING) {
      top = Math.max(VIEWPORT_PADDING, triggerRect.top - GAP - contentRect.height);
    }
  } else {
    top = triggerRect.top - GAP - contentRect.height;
    if (top < VIEWPORT_PADDING) {
      top = Math.min(vh - contentRect.height - VIEWPORT_PADDING, triggerRect.bottom + GAP);
    }
  }

  let left: number;
  if (placement.endsWith("end")) {
    left = triggerRect.right - contentRect.width;
  } else {
    left = triggerRect.left;
  }
  left = Math.max(
    VIEWPORT_PADDING,
    Math.min(left, vw - contentRect.width - VIEWPORT_PADDING),
  );

  content.style.top = `${top}px`;
  content.style.left = `${left}px`;
}

mount("[data-popover-root]", (root) => {
  const id = root.dataset.popoverId!;
  const trigger = root.querySelector<HTMLButtonElement>("[data-popover-trigger]");
  const content = root.querySelector<HTMLElement>("[data-popover-content]");

  if (!trigger || !content) return () => {};

  content.id = id;
  content.setAttribute("popover", "auto");
  trigger.setAttribute("popovertarget", id);
  trigger.setAttribute("aria-expanded", "false");

  // Every listener wired here is registered with this controller's signal, so
  // a single abort() in teardown removes the toggle listener and any window
  // scroll/resize listeners still attached from an open popover.
  const controller = new AbortController();
  const { signal } = controller;

  let reposition: (() => void) | null = null;

  content.addEventListener(
    "toggle",
    (e) => {
      const isOpen = (e as ToggleEvent).newState === "open";
      trigger.setAttribute("aria-expanded", String(isOpen));

      if (isOpen) {
        // Anchor twice — initial measurement, then once layout settles.
        anchor(trigger, content);
        requestAnimationFrame(() => anchor(trigger, content));

        reposition = () => anchor(trigger, content);
        window.addEventListener("scroll", reposition, { passive: true, capture: true, signal });
        window.addEventListener("resize", reposition, { passive: true, signal });
      } else if (reposition) {
        window.removeEventListener("scroll", reposition, { capture: true });
        window.removeEventListener("resize", reposition);
        reposition = null;
      }
    },
    { signal },
  );

  return () => controller.abort();
});
