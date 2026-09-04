/** Wires each API field group: expand/collapse-all + hash deep-linking. */

import { mount, initDisclosureGroup } from "@cloudflare/nimbus-docs/client";

mount("[data-api-section]", (root) => {
  const instance = initDisclosureGroup({
    root,
    toggleAll: root.querySelector<HTMLElement>("[data-api-toggle-all]"),
    deepLink: true,
  });
  return () => instance.destroy();
});
