/**
 * Mintlify built-in component names, backed by Nimbus components or compat
 * shims. Imported implicitly into snippet MDX by the Vite plugin and merged
 * into the MDX globals registry in src/components.ts.
 */
import { Steps, Step } from "@/components/ui/steps";
import { Tabs } from "@/components/ui/tabs";
import Note from "./Note.astro";
import Info from "./Info.astro";
import Tip from "./Tip.astro";
import Warning from "./Warning.astro";
import Danger from "./Danger.astro";
import Check from "./Check.astro";
import Tab from "./Tab.astro";
import Card from "./Card.astro";
import CardGroup from "./CardGroup.astro";
import Accordion from "./Accordion.astro";
import AccordionGroup from "./AccordionGroup.astro";
import Expandable from "./Expandable.astro";
import Frame from "./Frame.astro";
import Badge from "./Badge.astro";
import Tooltip from "./Tooltip.astro";
import Columns from "./Columns.astro";
import Update from "./Update.astro";
import Icon from "./Icon.astro";
import View from "./View.astro";
import Visibility from "./Visibility.astro";
import CodeBlock from "./CodeBlock.astro";

export {
  Note, Info, Tip, Warning, Danger, Check,
  Tabs, Tab, Steps, Step,
  Card, CardGroup, Accordion, AccordionGroup, Expandable,
  Frame, Badge, Tooltip, Columns, Update, Icon, View, Visibility, CodeBlock,
};

export const mintlifyGlobals = {
  Note, Info, Tip, Warning, Danger, Check,
  Tabs, Tab, Steps, Step,
  Card, CardGroup, Accordion, AccordionGroup, Expandable,
  Frame, Badge, Tooltip, Columns, Update, Icon, View, Visibility, CodeBlock,
};
