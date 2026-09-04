/**
 * Scroll-spy + animated rail indicator. Active heading tracked via a single
 * IntersectionObserver; the dash slides by arc-length so it weaves through the
 * rail's curves instead of cutting across.
 */

import { mount } from "@cloudflare/nimbus-docs/client";

const READING_BAND = 0.25;
const BOTTOM_EPSILON = 2;
const REVEAL_PADDING = 12;

function initToc(root: HTMLElement): () => void {
  const nav = root.querySelector<HTMLElement>("nav");
  const activePath = root.querySelector<SVGPathElement>("[data-nb-toc-rail-active]");
  if (!nav || !activePath) return () => {};

  const scrollHost = root.closest<HTMLElement>("[data-nb-toc-scroll-host]") ?? root;
  let links: HTMLElement[] = [];
  let slugs: string[] = [];
  let observed: { el: HTMLElement; index: number }[] = [];
  let indexOfEl = new Map<HTMLElement, number>();
  let spy: IntersectionObserver | undefined;
  const isVisible = (index: number) => !links[index]?.closest<HTMLElement>("li")?.hidden;
  const visibleIndexes = () => links.map((_, index) => index).filter(isVisible);

  let segments: { start: number; length: number }[] = [];
  let totalLength = 0;
  let currentIndex = -1;
  let currentLink: HTMLElement | null = null;
  let hasApplied = false;

  // Measure the rail from the DOM so the path stays pixel-perfect over the
  // static gray rail, capturing each link's arc-length range as we go.
  function buildRail() {
    const navRect = nav!.getBoundingClientRect();

    const m = links.map((link, index) => {
      const r = link.getBoundingClientRect();
      return {
        index,
        x: r.left - navRect.left + 1,
        yTop: r.top - navRect.top,
        yBot: r.top - navRect.top + r.height,
      };
    }).filter((item) => isVisible(item.index));

    let d = "";
    const newSegments: { start: number; length: number }[] = Array.from(
      { length: links.length },
      () => ({ start: 0, length: 0 }),
    );

    // Measure each command in isolation (O(1)) and accumulate, rather than
    // re-measuring the whole cumulatively-growing path with getTotalLength()
    // on every iteration — the latter is O(n^2) and blocks the main thread on
    // pages with hundreds of headings. Arc length is additive across
    // contiguous commands, so summing isolated sub-paths matches the total.
    // activePath doubles as the scratch measurer here; the full `d` is written
    // back once at the end.
    const measure = (subPath: string) => {
      activePath!.setAttribute("d", subPath);
      return activePath!.getTotalLength();
    };

    let cumulative = 0;
    let prevX = 0;
    let prevYBot = 0;

    for (let i = 0; i < m.length; i++) {
      const cur = m[i];

      if (i === 0) {
        d += `M ${cur.x} ${cur.yTop} `;
      } else {
        const prev = m[i - 1];
        let connector: string;
        if (Math.abs(cur.x - prev.x) < 0.5) {
          connector = `L ${cur.x} ${cur.yTop} `;
        } else {
          // Indent change → S-curve matching the static gap SVG.
          const midY = (prev.yBot + cur.yTop) / 2;
          connector = `C ${prev.x} ${midY}, ${cur.x} ${midY}, ${cur.x} ${cur.yTop} `;
        }
        d += connector;
        cumulative += measure(`M ${prevX} ${prevYBot} ${connector}`);
      }

      const start = cumulative;

      const seg = `L ${cur.x} ${cur.yBot} `;
      d += seg;
      cumulative += measure(`M ${cur.x} ${cur.yTop} ${seg}`);

      newSegments[cur.index] = { start, length: cumulative - start };

      prevX = cur.x;
      prevYBot = cur.yBot;
    }

    activePath!.setAttribute("d", d);
    segments = newSegments;
    totalLength = cumulative;
  }

  function applyActive(index: number, instant: boolean) {
    const seg = segments[index];
    if (!seg || seg.length === 0) return;

    if (instant) {
      activePath!.setAttribute("data-initial", "true");
      // Force recalc so only opacity transitions on first paint (no dash sweep).
      void activePath!.getBoundingClientRect();
    }

    activePath!.style.strokeDasharray = `${seg.length} ${totalLength + 1}`;
    activePath!.style.strokeDashoffset = `${-seg.start}`;

    if (instant) {
      requestAnimationFrame(() => {
        activePath!.setAttribute("data-ready", "true");
        requestAnimationFrame(() => {
          activePath!.removeAttribute("data-initial");
        });
      });
    }
  }

  function revealActiveLink(link: HTMLElement) {
    const hostRect = scrollHost.getBoundingClientRect();
    const linkRect = link.getBoundingClientRect();

    if (linkRect.top < hostRect.top + REVEAL_PADDING) {
      scrollHost.scrollTop += linkRect.top - hostRect.top - REVEAL_PADDING;
      return;
    }

    if (linkRect.bottom > hostRect.bottom - REVEAL_PADDING) {
      scrollHost.scrollTop += linkRect.bottom - hostRect.bottom + REVEAL_PADDING;
    }
  }

  function setActive(index: number) {
    if (index === currentIndex) return;
    currentIndex = index;

    currentLink?.removeAttribute("aria-current");
    const activeLink = links[index] ?? null;
    activeLink?.setAttribute("aria-current", "true");
    currentLink = activeLink;
    if (activeLink) revealActiveLink(activeLink);

    applyActive(index, !hasApplied);
    hasApplied = true;
  }

  const inBand = new Set<number>();
  let observedIndex = 0;
  let atBottom = false;
  let pinnedIndex: number | null = null;
  let pinnedEnteredViewport = false;

  function resolve() {
    if (pinnedIndex !== null) {
      setActive(pinnedIndex);
      return;
    }
    const visible = visibleIndexes();
    if (visible.length === 0) return;
    if (atBottom) {
      setActive(visible[visible.length - 1]);
      return;
    }
    let target = visible[0];
    for (const index of visible) {
      if (index > observedIndex) break;
      target = index;
    }
    setActive(target);
  }

  // Root margin collapses the root to the top band; deepest in-band heading
  // wins. View changes can replace the TOC links, so reconnect the observer
  // to the headings that belong to the newly selected view.
  function connectObserver() {
    spy?.disconnect();
    links = Array.from(root.querySelectorAll<HTMLElement>("[data-nb-toc-link]"));
    slugs = links.map((link) => link.dataset.nbSlug ?? "");
    observed = slugs
      .map((slug, index) => ({ el: document.getElementById(slug), index }))
      .filter((item): item is { el: HTMLElement; index: number } => item.el !== null);
    indexOfEl = new Map(observed.map((item) => [item.el, item.index]));

    spy = new IntersectionObserver((entries) => {
      for (const entry of entries) {
        const i = indexOfEl.get(entry.target as HTMLElement);
        if (i === undefined || !isVisible(i)) continue;
        if (entry.isIntersecting) inBand.add(i);
        else inBand.delete(i);
      }
      if (inBand.size > 0) observedIndex = Math.max(...inBand);
      resolve();
    }, {
      rootMargin: `0px 0px -${(1 - READING_BAND) * 100}% 0px`,
      threshold: 0,
    });
    observed.forEach((item) => spy.observe(item.el));
  }

  function updateBottom() {
    const scrollEl = document.scrollingElement ?? document.documentElement;
    const maxScroll = scrollEl.scrollHeight - window.innerHeight;
    const next =
      maxScroll > BOTTOM_EPSILON &&
      scrollEl.scrollTop >= maxScroll - BOTTOM_EPSILON;
    if (next !== atBottom) {
      atBottom = next;
      resolve();
    }
  }

  function updateObservedIndex() {
    const bandBottom = window.innerHeight * READING_BAND;
    let nextIndex = visibleIndexes()[0] ?? 0;
    for (const o of observed) {
      if (!isVisible(o.index)) continue;
      if (o.el.getBoundingClientRect().top <= bandBottom) nextIndex = o.index;
      else break;
    }
    observedIndex = nextIndex;
  }

  function releaseStalePin() {
    if (pinnedIndex === null) return;
    if (!isVisible(pinnedIndex)) {
      pinnedIndex = null;
      pinnedEnteredViewport = false;
      return;
    }
    const heading = document.getElementById(slugs[pinnedIndex]);
    if (!heading) {
      pinnedIndex = null;
      pinnedEnteredViewport = false;
      return;
    }

    const rect = heading.getBoundingClientRect();
    const inViewport = rect.bottom >= 0 && rect.top <= window.innerHeight;
    if (inViewport) {
      pinnedEnteredViewport = true;
      return;
    }

    if (pinnedEnteredViewport) {
      pinnedIndex = null;
      pinnedEnteredViewport = false;
    }
  }

  let ticking = false;
  function onScroll() {
    if (ticking) return;
    ticking = true;
    requestAnimationFrame(() => {
      updateObservedIndex();
      updateBottom();
      releaseStalePin();
      resolve();
      ticking = false;
    });
  }

  function onLayoutChange() {
    buildRail();
    updateObservedIndex();
    updateBottom();
    releaseStalePin();
    resolve();
    if (currentIndex >= 0) {
      applyActive(currentIndex, true);
      const activeLink = links[currentIndex];
      if (activeLink) revealActiveLink(activeLink);
    }
  }

  const controller = new AbortController();

  function onViewChange() {
    inBand.clear();
    pinnedIndex = null;
    pinnedEnteredViewport = false;
    currentLink?.removeAttribute("aria-current");
    currentLink = null;
    currentIndex = -1;
    hasApplied = false;
    connectObserver();
    onLayoutChange();
  }

  nav.addEventListener(
    "click",
    (e) => {
      if (e.defaultPrevented || e.button !== 0 || e.metaKey || e.ctrlKey || e.shiftKey || e.altKey) return;
      const link = (e.target as Element).closest<HTMLElement>("[data-nb-toc-link]");
      if (!link) return;
      const i = slugs.indexOf(link.dataset.nbSlug!);
      if (i === -1) return;
      pinnedIndex = i;
      const heading = document.getElementById(slugs[i]);
      const rect = heading?.getBoundingClientRect();
      pinnedEnteredViewport = !!rect && rect.bottom >= 0 && rect.top <= window.innerHeight;
      resolve();
    },
    { signal: controller.signal },
  );

  // Hand-driven scrolling releases the pin and resumes auto-tracking.
  function releasePin() {
    if (pinnedIndex === null) return;
    pinnedIndex = null;
    pinnedEnteredViewport = false;
    resolve();
  }
  const NAV_KEYS = new Set([
    "ArrowUp",
    "ArrowDown",
    "PageUp",
    "PageDown",
    "Home",
    "End",
    " ",
    "Spacebar",
  ]);
  window.addEventListener("wheel", releasePin, {
    passive: true,
    signal: controller.signal,
  });
  window.addEventListener("touchmove", releasePin, {
    passive: true,
    signal: controller.signal,
  });
  window.addEventListener(
    "keydown",
    (e) => {
      if (NAV_KEYS.has(e.key)) releasePin();
    },
    { signal: controller.signal },
  );

  window.addEventListener("scroll", onScroll, {
    passive: true,
    signal: controller.signal,
  });
  window.addEventListener("resize", onLayoutChange, {
    passive: true,
    signal: controller.signal,
  });
  window.addEventListener("ch:view-change", onViewChange, {
    signal: controller.signal,
  });

  const ro = new ResizeObserver(onLayoutChange);
  ro.observe(nav);

  connectObserver();
  buildRail();
  updateObservedIndex();
  updateBottom();
  resolve();

  return () => {
    controller.abort();
    ro.disconnect();
    spy?.disconnect();
  };
}

mount("[data-nb-toc]", initToc);
