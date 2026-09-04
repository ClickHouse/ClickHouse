/**
 * steps.client.ts — Safari list-role restoration.
 *
 * Safari strips list semantics when `list-style: none` is applied
 * (which we do for the numbered counter styling). Restoring `role="list"`
 * on the inner `<ol>` makes VoiceOver announce the item count again.
 */

import { mount } from "@cloudflare/nimbus-docs/client";

function initSteps(root: HTMLElement): () => void {
  const lists = root.querySelectorAll<HTMLOListElement>("ol");
  if (
    import.meta.env.DEV &&
    lists.length === 0 &&
    root.querySelector("[data-step]") === null &&
    root.children.length > 0
  ) {
    console.warn(
      "[nimbus] <Steps> expects an ordered list (`1.` items) or <Step> " +
        "children. A bullet list renders with no numbers or connectors — " +
        "use an ordered list.",
    );
  }
  lists.forEach((ol) => ol.setAttribute("role", "list"));

  return () => {
    lists.forEach((ol) => ol.removeAttribute("role"));
  };
}

mount("[data-steps]", initSteps);
