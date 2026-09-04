/**
 * MDX globals registry: components available inside MDX without `import`.
 * Nimbus's own set plus the Mintlify built-in names the existing content
 * uses (see src/components/compat/). Wired via `<Content components={components} />`.
 */
import { Aside } from "./components/ui/aside";
import Render from "./components/Render.astro";
import { CardGrid } from "./components/ui/card-grid";
import { PackageManagers } from "./components/ui/package-managers";
import { TabItem } from "./components/ui/tabs";
import { mintlifyGlobals } from "./components/compat/globals";

export const components = {
  Aside,
  CardGrid,
  PackageManagers,
  Render,
  TabItem,
  ...mintlifyGlobals,
};
