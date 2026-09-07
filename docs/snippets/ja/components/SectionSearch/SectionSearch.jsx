export const SectionSearch = ({ section = "/reference/statements", label = "Statements", placeholder = "このセクションを検索..." }) => {
  const ref = useRef(null)
  useEffect(() => {
    const el = ref.current
    if (!el || el.dataset.inkeepMounted === "1") return
    // Unique id per mount so a re-mount (React tears down + rebuilds this
    // subtree on a theme toggle) never collides with a briefly-lingering
    // previous target of the same id, which left the widget mounting into a
    // stale node — the box vanishing after a couple of toggles.
    window.__sectionSearchSeq = (window.__sectionSearchSeq || 0) + 1
    const TARGET_ID = "inkeep-section-search-" + window.__sectionSearchSeq
    el.id = TARGET_ID

    // Public, client-side Inkeep keys (same values as inkeep-init.js — safe to
    // ship in the browser).
    const KEY_STAGING = "d3e2792740610240ff7bcf2c2a78a33012812eb4f3e34d54"
    const KEY_LOCAL = "b25e5cf856ec9da60d250578b59dace8417359feeedcbc6b"
    const apiKey = /\.mintlify\.app$/.test(window.location.hostname) ? KEY_STAGING : KEY_LOCAL
    const EMBED = "https://cdn.jsdelivr.net/npm/@inkeep/cxkit-js@0.5/dist/embed.js"
    const PREVIEW_HOST = "private-7c7dfe99.mintlify.app"
    const PREVIEW_RE = new RegExp("^https?://" + PREVIEW_HOST.replace(/\./g, "\\.") + "/")

    // Reduce any result URL (preview host or canonical) to a bare docs path.
    const toPath = (u) =>
      (u || "")
        .replace(PREVIEW_RE, "https://clickhouse.com/docs/")
        .replace(/^https?:\/\/clickhouse\.com\/docs/, "")
        .replace(/^https?:\/\/[^/]+/, "")
        .replace(/[?#].*$/, "")

    const config = {
      baseSettings: {
        apiKey,
        organizationDisplayName: "ClickHouse",
        primaryBrandColor: "#fdff75",
        // Rewrite preview links to canonical docs, and keep ONLY results whose
        // path is under `section`. Out-of-section results get no tab, so — with
        // a single configured tab — they render nowhere. Same tab-tagging
        // scoping the global modal uses (inkeep-init.js).
        transformSource: (source) => {
          let url = source.url || ""
          if (PREVIEW_RE.test(url)) url = url.replace(PREVIEW_RE, "https://clickhouse.com/docs/")
          const path = toPath(url)
          const inSection = path === section || path.indexOf(section + "/") === 0
          return Object.assign({}, source, { url, tabs: inSection ? [label] : [] })
        },
        // Follow Mintlify's `.dark` class on <html>.
        colorMode: {
          sync: {
            target: document.documentElement,
            attributes: ["class"],
            isDarkMode: (a) => ((a && a.class) || "").indexOf("dark") !== -1
          }
        },
        theme: {
          styles: [
            // Only one tab exists, so hide the tab bar for a clean inline look.
            { key: "hide-single-tab", type: "style", value: ".ikp-ai-search-results__tab-list { display: none !important; }" },
            // Inkeep leaves the input row (`.ikp-ai-search-input-group`)
            // unstyled, so match Mintlify's own site-search button exactly:
            // rounded-xl, bg-background-light/dark, and a 1px ring (box-shadow,
            // not a border). Background references the same --background-*
            // custom properties Mintlify sets (they cascade into the shadow
            // root); ring colors are the button's fixed Tailwind gray/opacity
            // values. `.dark` is applied by Inkeep inside the shadow root.
            {
              key: "section-search-box",
              type: "style",
              value: [
                ".ikp-ai-search-input-group {",
                "  border-radius: 6px;" /* box corner radius */,
                "  background: var(--background-light, #ffffff);" /* bg-background-light (docs.json) */,
                "  box-shadow: 0 0 0 1px rgb(156 163 175 / 0.3);" /* ring-gray-400/30 */,
                "  padding-top: 0;" /* drop the tall pt-3/pb-3 padding */,
                "  padding-bottom: 0;" /* so the row height tracks the input */,
                "  min-height: 2.25rem;" /* h-9, matching the site search button */,
                "}",
                ".ikp-ai-search-input { min-height: 0; }" /* was min-h-[38px]; let it match h-9 */,
                /* Offset the results list from the search bar. */
                ".ikp-ai-search-results__list { margin-top: 0.5rem !important; }",
                ".ikp-ai-search-input-group:hover {",
                "  box-shadow: 0 0 0 1px rgb(75 85 99 / 0.3);" /* hover:ring-gray-600/30 */,
                "}",
                ".dark .ikp-ai-search-input-group {",
                "  background: var(--background-dark, #151515);" /* dark:bg-background-dark (docs.json) */,
                "  box-shadow: 0 0 0 1px rgb(75 85 99 / 0.3);" /* dark:ring-gray-600/30 */,
                "  filter: brightness(1.1);" /* dark:brightness-[1.1] */,
                "}",
                ".dark .ikp-ai-search-input-group:hover {",
                "  box-shadow: 0 0 0 1px rgb(107 114 128 / 0.3);" /* dark:hover:ring-gray-500/30 */,
                "  filter: brightness(1.25);" /* dark:hover:brightness-[1.25] */,
                "}"
              ].join("\n")
            }
          ]
        }
      },
      searchSettings: {
        placeholder,
        debounceTimeMs: 300,
        maxResults: 20,
        shouldShowContentSnippets: true,
        contentSnippetLength: 200,
        shouldHighlightMatches: true,
        tabs: [label]
      }
    }

    let cancelled = false
    let widget = null

    const mount = () => {
      if (cancelled || el.dataset.inkeepMounted === "1") return
      // Prefer the trapped cxkit-js global; fall back to window.Inkeep when we
      // didn't trap (no modal present to protect).
      const api = window.__inkeepEmbedJs || window.Inkeep
      if (!api || typeof api.EmbeddedSearch !== "function") {
        requestAnimationFrame(mount)
        return
      }
      el.dataset.inkeepMounted = "1"
      try {
        widget = api.EmbeddedSearch("#" + TARGET_ID, config)
      } catch (e) {
        console.log("Inkeep section search failed:", e)
      }
    }

    const loadEmbed = (trap) => {
      // cxkit-js's embed.js does `window.Inkeep = ...` on load, which would
      // clobber the site-wide modal's global (also window.Inkeep). When the
      // modal is present, trap that assignment into __inkeepEmbedJs and keep
      // window.Inkeep returning the modal's object, so global search is never
      // disturbed.
      if (trap && !window.__inkeepTrapped) {
        const modalInkeep = window.Inkeep
        // A slow modal load can arrive after embed.js has already populated
        // window.Inkeep. Preserve that usable inline-search API before the
        // trap hides it, otherwise a later remount polls the modal forever.
        if (!window.__inkeepEmbedJs && modalInkeep && typeof modalInkeep.EmbeddedSearch === "function") {
          window.__inkeepEmbedJs = modalInkeep
        }
        try {
          Object.defineProperty(window, "Inkeep", {
            configurable: true,
            get: function () {
              return modalInkeep
            },
            set: function (v) {
              window.__inkeepEmbedJs = v
            }
          })
          window.__inkeepTrapped = true
        } catch (e) {
          /* fall back to shared global */
        }
      }
      if (!document.getElementById("inkeep-embed-js-script")) {
        const s = document.createElement("script")
        s.id = "inkeep-embed-js-script"
        s.type = "module"
        s.src = EMBED
        s.onload = () => {
          // Retain the embed API even if the modal bundle later replaces the
          // shared window.Inkeep global before this component remounts.
          const embedApi = window.__inkeepEmbedJs || window.Inkeep
          if (embedApi && typeof embedApi.EmbeddedSearch === "function") {
            window.__inkeepEmbedJs = embedApi
          }
          mount()
        }
        document.head.appendChild(s)
      }
      mount()
    }

    // Wait for the modal bundle to finish first so trapping the global is safe;
    // after ~8s with no modal (e.g. Inkeep disabled), load without a trap and
    // just share window.Inkeep.
    let frames = 0
    const waitForModal = () => {
      if (cancelled) return
      if (window.__inkeepEmbedJs) {
        mount()
        return
      }
      const modalReady = window.Inkeep && typeof window.Inkeep.ModalSearchAndChat === "function"
      if (modalReady) {
        loadEmbed(true)
        return
      }
      if (frames > 480) {
        loadEmbed(false)
        return
      }
      frames++
      requestAnimationFrame(waitForModal)
    }
    waitForModal()

    return () => {
      cancelled = true
      // Mintlify unmounts/remounts this component on a theme toggle. Tear the
      // widget down and clear the guard so the remount starts clean, instead
      // of orphaning a detached instance (which shows as the box disappearing).
      try {
        if (widget && typeof widget.unmount === "function") widget.unmount()
        else if (widget && typeof widget.destroy === "function") widget.destroy()
      } catch (e) {
        /* ignore */
      }
      if (el) el.dataset.inkeepMounted = ""
    }
  }, [section, label, placeholder])

  return <div ref={ref} className="not-prose" style={{ margin: "1.25rem 0" }} />
}