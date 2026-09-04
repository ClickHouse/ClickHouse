/**
 * Mobile TOC — keeps the "jump to section" <select> in sync with the page.
 *
 *   - select → page: on change, scroll to the chosen heading and suppress the
 *     observer briefly so the value doesn't flicker while the page scrolls to
 *     the target.
 *   - page → select: an IntersectionObserver mirrors the active heading back
 *     into the select value — the topmost heading inside the reading band, or
 *     the first/last heading clamped by scroll position when none intersect.
 *
 * Teardown via AbortController for view transitions; a persistent in-band set
 * (like the desktop rail in `toc.client.ts`) so the active heading is stable
 * when several fall inside the band at once; rect-based clamping so it doesn't
 * rely on `offsetParent` layout.
 */

import { mount } from "@cloudflare/nimbus-docs/client";

// Ignore the top 10% and bottom 70% of the viewport so the "active" heading is
// whatever sits near the top of the reading area. The reading band is [10%,
// 30%] of the viewport height; BAND_TOP is its upper edge, reused by the
// first/last clamp below.
const BAND_TOP = 0.1;
const ROOT_MARGIN = "-10% 0px -70% 0px";
const SUPPRESS_MS = 1000;

function prefersReducedMotion(): boolean {
  return window.matchMedia("(prefers-reduced-motion: reduce)").matches;
}

function initMobileToc(root: HTMLElement): () => void {
  const select = root.querySelector<HTMLSelectElement>(
    "[data-nb-mobile-toc-select]",
  );
  if (!select) return () => {};

  // Paired so slug/element indices stay aligned; `inBand` indexes into this.
  type Heading = { slug: string; el: HTMLElement };
  const headings: Heading[] = Array.from(select.options)
    .map((o) => o.value)
    .filter((v) => v !== "_top")
    .map((slug) => ({ slug, el: document.getElementById(slug) }))
    .filter((h): h is Heading => h.el !== null);

  const controller = new AbortController();

  // While true, observer callbacks are ignored so a click-driven scroll
  // doesn't fight the value we just set.
  let suppress = false;
  let suppressTimer: ReturnType<typeof setTimeout> | undefined;

  function setActive(slug: string) {
    if (select!.value !== slug) select!.value = slug;
  }

  // select → page
  select.addEventListener(
    "change",
    () => {
      const slug = select.value;
      suppress = true;
      clearTimeout(suppressTimer);
      suppressTimer = setTimeout(() => {
        suppress = false;
      }, SUPPRESS_MS);

      const behavior: ScrollBehavior = prefersReducedMotion()
        ? "auto"
        : "smooth";
      if (slug === "_top") {
        window.scrollTo({ top: 0, behavior });
        return;
      }
      document.getElementById(slug)?.scrollIntoView({ behavior });
    },
    { signal: controller.signal },
  );

  if (headings.length === 0) {
    return () => {
      controller.abort();
      clearTimeout(suppressTimer);
    };
  }

  // page → select. Track every heading currently inside the band so the active
  // one is stable when multiple short sections share it.
  const inBand = new Set<number>();

  function resolve() {
    if (suppress) return;

    if (inBand.size > 0) {
      // Topmost in-band heading (smallest document-order index).
      setActive(headings[Math.min(...inBand)].slug);
      return;
    }

    // Nothing in the band — clamp to the first or last heading based on where
    // the boundary headings sit relative to the band; otherwise keep the
    // current value (we're mid-section between two headings).
    const bandTop = window.innerHeight * BAND_TOP;
    const firstTop = headings[0].el.getBoundingClientRect().top;
    const lastTop =
      headings[headings.length - 1].el.getBoundingClientRect().top;
    if (firstTop > bandTop) {
      setActive("_top");
    } else if (lastTop < bandTop) {
      setActive(headings[headings.length - 1].slug);
    }
  }

  const observer = new IntersectionObserver(
    (entries) => {
      for (const entry of entries) {
        const i = headings.findIndex((h) => h.el === entry.target);
        if (i === -1) continue;
        if (entry.isIntersecting) inBand.add(i);
        else inBand.delete(i);
      }
      resolve();
    },
    { rootMargin: ROOT_MARGIN, threshold: 0 },
  );

  for (const { el } of headings) observer.observe(el);
  resolve(); // initial sync before the observer's first async callback

  return () => {
    controller.abort();
    observer.disconnect();
    clearTimeout(suppressTimer);
  };
}

mount("[data-nb-mobile-toc]", initMobileToc);
