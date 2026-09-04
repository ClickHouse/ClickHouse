/** Wires Collapsible via the disclosure module. */

import { mount, makeDisclosure } from "@cloudflare/nimbus-docs/client";

function initCollapsible(root: HTMLElement): () => void {
  // A collapsible can contain other collapsibles. Only bind controls owned by
  // this root; a descendant trigger must never toggle an ancestor's panel.
  const trigger = Array.from(root.querySelectorAll<HTMLElement>("[data-nb-collapsible-trigger]"))
    .find((candidate) => candidate.closest("[data-nb-collapsible]") === root);
  const content = Array.from(root.querySelectorAll<HTMLElement>("[data-nb-collapsible-content]"))
    .find((candidate) => candidate.closest("[data-nb-collapsible]") === root);

  if (!trigger || !content) return () => {};

  const defaultOpen = root.dataset.nbDefaultOpen === "true";

  const disclosure = makeDisclosure({
    trigger,
    content,
    defaultOpen,
  });

  return () => disclosure.destroy();
}

mount("[data-nb-collapsible]", initCollapsible);
