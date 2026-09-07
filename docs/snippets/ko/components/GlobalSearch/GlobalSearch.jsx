export const GlobalSearch = ({ placeholder = "ClickHouse 문서 검색..." }) => {
  // Mintlify는 가져온 컴포넌트를 독립 실행형 함수로 평가하므로,
  // 컴포넌트에서 사용하는 헬퍼는 컴포넌트 스코프 내에 위치해야 합니다.
  const transformSource = (source) => {
    let url = source.url || ""
    const isPreview = url.indexOf("private-7c7dfe99.mintlify.app") !== -1
    if (isPreview) {
      url = url.replace(/^https?:\/\/private-7c7dfe99\.mintlify\.app\//, "https://clickhouse.com/docs/")
    }
    const tabs = []
    if (isPreview || /clickhouse\.com\/docs(\/|$)/.test(url)) {
      tabs.push("Docs")
    } else if (url.indexOf("github.com") !== -1 && /\/issues(\/|$)/.test(url)) tabs.push("Docs")
    else if (/\/blog(\/|$)/.test(url)) tabs.push("Docs")
    else if (url.indexOf("clickhouse.com") !== -1) tabs.push("Docs")
    return Object.assign({}, source, { tabs, url })
  }

  const ref = useRef(null)
  useEffect(() => {
    const el = ref.current
    if (!el || el.dataset.inkeepMounted === "1") return
    // 마운트할 때마다 고유한 id를 사용합니다. Mintlify는 테마 변경 시
    // 이 서브트리를 다시 마운트하며, 이전 대상이 DOM에 잠시 남아 있을 수 있습니다.
    window.__globalSearchSeq = (window.__globalSearchSeq || 0) + 1
    const targetId = "inkeep-global-search-" + window.__globalSearchSeq
    el.id = targetId

    // 공개 클라이언트 측 Inkeep 키로, inkeep-init.js에서도 사용됩니다.
    const keyStaging = "d3e2792740610240ff7bcf2c2a78a33012812eb4f3e34d54"
    const keyLocal = "b25e5cf856ec9da60d250578b59dace8417359feeedcbc6b"
    const apiKey = /\.mintlify\.app$/.test(window.location.hostname) ? keyStaging : keyLocal
    const embedUrl = "https://cdn.jsdelivr.net/npm/@inkeep/cxkit-js@0.5/dist/embed.js"
    const config = {
      baseSettings: {
        apiKey,
        organizationDisplayName: "ClickHouse",
        primaryBrandColor: "#fdff75",
        transformSource,
        colorMode: {
          sync: {
            target: document.documentElement,
            attributes: ["class"],
            isDarkMode: (attributes) => ((attributes && attributes.class) || "").indexOf("dark") !== -1
          }
        },
        theme: {
          styles: [
            {
              key: "hide-single-tab",
              type: "style",
              value: ".ikp-ai-search-results__tab-list { display: none !important; }"
            },
            {
              key: "section-search-box",
              type: "style",
              value: [
                ".ikp-ai-search-input-group {",
                "  border-radius: 6px;",
                "  background: var(--background-light, #ffffff);",
                "  box-shadow: 0 0 0 1px rgb(156 163 175 / 0.3);",
                "  padding-top: 0;",
                "  padding-bottom: 0;",
                "  min-height: 2.25rem;",
                "}",
                ".ikp-ai-search-input { min-height: 0; }",
                ".ikp-ai-search-results__list { margin-top: 0.5rem !important; }",
                ".ikp-ai-search-input-group:hover {",
                "  box-shadow: 0 0 0 1px rgb(75 85 99 / 0.3);",
                "}",
                ".dark .ikp-ai-search-input-group {",
                "  background: var(--background-dark, #151515);",
                "  box-shadow: 0 0 0 1px rgb(75 85 99 / 0.3);",
                "  filter: brightness(1.1);",
                "}",
                ".dark .ikp-ai-search-input-group:hover {",
                "  box-shadow: 0 0 0 1px rgb(107 114 128 / 0.3);",
                "  filter: brightness(1.25);",
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
        tabs: ["Docs"]
      }
    }

    let cancelled = false
    let widget = null

    const mount = () => {
      if (cancelled || el.dataset.inkeepMounted === "1") return
      const api = window.__inkeepEmbedJs || window.Inkeep
      if (!api || typeof api.EmbeddedSearch !== "function") {
        requestAnimationFrame(mount)
        return
      }
      el.dataset.inkeepMounted = "1"
      try {
        widget = api.EmbeddedSearch("#" + targetId, config)
      } catch (error) {
        console.log("Inkeep 전역 검색 실패:", error)
      }
    }

    const loadEmbed = (trap) => {
      // 임베드와 사이트 전체 모달 모두 window.Inkeep을 할당합니다. 임베디드 검색 API는
      // 별도로 유지하면서 모달 전역 변수를 보존합니다.
      if (trap && !window.__inkeepTrapped) {
        const modalInkeep = window.Inkeep
        if (!window.__inkeepEmbedJs && modalInkeep && typeof modalInkeep.EmbeddedSearch === "function") {
          window.__inkeepEmbedJs = modalInkeep
        }
        try {
          Object.defineProperty(window, "Inkeep", {
            configurable: true,
            get: function () {
              return modalInkeep
            },
            set: function (value) {
              window.__inkeepEmbedJs = value
            }
          })
          window.__inkeepTrapped = true
        } catch (error) {
          // 공유 전역 변수로 폴백합니다.
        }
      }
      if (!document.getElementById("inkeep-embed-js-script")) {
        const script = document.createElement("script")
        script.id = "inkeep-embed-js-script"
        script.type = "module"
        script.src = embedUrl
        script.onload = () => {
          const embedApi = window.__inkeepEmbedJs || window.Inkeep
          if (embedApi && typeof embedApi.EmbeddedSearch === "function") {
            window.__inkeepEmbedJs = embedApi
          }
          mount()
        }
        document.head.appendChild(script)
      }
      mount()
    }

    // 전역 변수를 가로채기 전에 모달 번들 로드를 기다립니다. 모달이
    // 비활성화된 경우, 약 8초 후 window.Inkeep을 공유합니다.
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
      try {
        if (widget && typeof widget.unmount === "function") widget.unmount()
        else if (widget && typeof widget.destroy === "function") widget.destroy()
      } catch (error) {
        // 이미 분리된 위젯의 정리 오류는 무시합니다.
      }
      if (el) el.dataset.inkeepMounted = ""
    }
  }, [placeholder])

  return <div ref={ref} className="not-prose" style={{ margin: "1.25rem 0" }} />
}