export const IntegrationGrid = () => {
  // Custom hook to detect dark mode
  function useDarkMode() {
    const [isDark, setIsDark] = useState(false)

    useEffect(() => {
      const checkDarkMode = () => {
        const theme = document.documentElement.getAttribute("data-theme") || document.body.getAttribute("data-theme")
        if (theme === "dark") {
          setIsDark(true)
          return
        }
        if (theme === "light") {
          setIsDark(false)
          return
        }

        if (document.documentElement.classList.contains("dark") || document.body.classList.contains("dark")) {
          setIsDark(true)
          return
        }

        setIsDark(window.matchMedia("(prefers-color-scheme: dark)").matches)
      }

      checkDarkMode()

      const observer = new MutationObserver(checkDarkMode)
      observer.observe(document.documentElement, {
        attributes: true,
        attributeFilter: ["data-theme", "class"]
      })
      observer.observe(document.body, {
        attributes: true,
        attributeFilter: ["data-theme", "class"]
      })

      const mediaQuery = window.matchMedia("(prefers-color-scheme: dark)")
      const handleChange = () => checkDarkMode()
      mediaQuery.addEventListener("change", handleChange)

      return () => {
        observer.disconnect()
        mediaQuery.removeEventListener("change", handleChange)
      }
    }, [])

    return isDark
  }

  function getTierIcon(tier, withMargin = false) {
    const style = withMargin ? { marginRight: "6px" } : {}

    switch (tier) {
      case "core":
        return (
          <svg width="20" height="20" viewBox="0 0 16 16" fill="none" xmlns="http://www.w3.org/2000/svg" style={style}>
            <path
              d="M1.30762 1.39073C1.30762 1.3103 1.37465 1.22986 1.46849 1.22986H2.64824C2.72868 1.22986 2.80912 1.29689 2.80912 1.39073V14.4886C2.80912 14.5691 2.74209 14.6495 2.64824 14.6495H1.46849C1.38805 14.6495 1.30762 14.5825 1.30762 14.4886V1.39073Z"
              fill="currentColor"
            />
            <path
              d="M4.2832 1.39073C4.2832 1.3103 4.35023 1.22986 4.44408 1.22986H5.62383C5.70427 1.22986 5.7847 1.29689 5.7847 1.39073V14.4886C5.7847 14.5691 5.71767 14.6495 5.62383 14.6495H4.44408C4.36364 14.6495 4.2832 14.5825 4.2832 14.4886V1.39073Z"
              fill="currentColor"
            />
            <path
              d="M7.25977 1.39073C7.25977 1.3103 7.3268 1.22986 7.42064 1.22986H8.60039C8.68083 1.22986 8.76127 1.29689 8.76127 1.39073V14.4886C8.76127 14.5691 8.69423 14.6495 8.60039 14.6495H7.42064C7.3402 14.6495 7.25977 14.5825 7.25977 14.4886V1.39073Z"
              fill="currentColor"
            />
            <path
              d="M10.2354 1.39073C10.2354 1.3103 10.3024 1.22986 10.3962 1.22986H11.576C11.6564 1.22986 11.7369 1.29689 11.7369 1.39073V14.4886C11.7369 14.5691 11.6698 14.6495 11.576 14.6495H10.3962C10.3158 14.6495 10.2354 14.5825 10.2354 14.4886V1.39073Z"
              fill="currentColor"
            />
            <path
              d="M13.2256 6.6057C13.2256 6.52526 13.2926 6.44482 13.3865 6.44482H14.5662C14.6466 6.44482 14.7271 6.51186 14.7271 6.6057V9.27354C14.7271 9.35398 14.6601 9.43442 14.5662 9.43442H13.3865C13.306 9.43442 13.2256 9.36739 13.2256 9.27354V6.6057Z"
              fill="currentColor"
            />
          </svg>
        )
      case "partner":
        return (
          <svg width="20" height="20" viewBox="0 0 16 16" fill="none" xmlns="http://www.w3.org/2000/svg" style={style}>
            <polyline points="12.5 9.5 10 12 6 11 2.5 8.5" stroke="currentColor" strokeLinecap="round" strokeLinejoin="round" strokeWidth="1" />
            <polyline points="4.54 4.41 8 3.5 11.46 4.41" stroke="currentColor" strokeLinecap="round" strokeLinejoin="round" strokeWidth="1" />
            <path
              d="M2.15,3.78 L0.55,6.95 A0.5,0.5 0,0,0 0.77,7.62 L2.5,8.5 L4.54,4.41 L2.82,3.55 A0.5,0.5 0,0,0 2.15,3.78 Z"
              stroke="currentColor"
              strokeLinecap="round"
              strokeLinejoin="round"
              strokeWidth="1"
            />
            <path
              d="M13.5,8.5 L15.23,7.62 A0.5,0.5 0,0,0 15.45,6.95 L13.85,3.78 A0.5,0.5 0,0,0 13.18,3.55 L11.46,4.41 Z"
              stroke="currentColor"
              strokeLinecap="round"
              strokeLinejoin="round"
              strokeWidth="1"
            />
            <path
              d="M11.5,4.5 L9,4.5 L6.15,7.27 A0.5,0.5 0,0,0 6.24,8.05 C7.33,8.74 8.81,8.72 10,7.5 L12.5,9.5 L13.5,8.5"
              stroke="currentColor"
              strokeLinecap="round"
              strokeLinejoin="round"
              strokeWidth="1"
            />
            <polyline points="7.75 13.5 5.15 12.85 3.5 11.67" stroke="currentColor" strokeLinecap="round" strokeLinejoin="round" strokeWidth="1" />
          </svg>
        )
      case "community":
        return (
          <svg width="20" height="20" viewBox="0 0 16 16" fill="none" xmlns="http://www.w3.org/2000/svg" style={style}>
            <path
              fillRule="evenodd"
              clipRule="evenodd"
              d="M6.22168 4.44463V4.44463C6.22168 3.46263 7.01768 2.66663 7.99968 2.66663V2.66663C8.98168 2.66663 9.77768 3.46263 9.77768 4.44463V4.44463C9.77768 5.42663 8.98168 6.22263 7.99968 6.22263V6.22263C7.01768 6.22196 6.22168 5.42596 6.22168 4.44463Z"
              stroke="currentColor"
              strokeLinecap="round"
              strokeLinejoin="round"
            />
            <path
              fillRule="evenodd"
              clipRule="evenodd"
              d="M1.91309 11.5553V11.5553C1.91309 10.5733 2.70909 9.77734 3.69109 9.77734V9.77734C4.67309 9.77734 5.46909 10.5733 5.46909 11.5553V11.5553C5.46842 12.5373 4.67309 13.3333 3.69109 13.3333V13.3333C2.70909 13.3333 1.91309 12.5373 1.91309 11.5553Z"
              stroke="currentColor"
              strokeLinecap="round"
              strokeLinejoin="round"
            />
            <path
              fillRule="evenodd"
              clipRule="evenodd"
              d="M10.5322 11.5553V11.5553C10.5322 10.5733 11.3282 9.77734 12.3102 9.77734V9.77734C13.2922 9.77734 14.0882 10.5733 14.0882 11.5553V11.5553C14.0882 12.5373 13.2922 13.3333 12.3102 13.3333V13.3333C11.3276 13.3333 10.5322 12.5373 10.5322 11.5553H10.5322Z"
              stroke="currentColor"
              strokeLinecap="round"
              strokeLinejoin="round"
            />
            <path d="M10.5939 11.1134H5.40723" stroke="currentColor" strokeLinecap="round" strokeLinejoin="round" />
            <path d="M8.95996 5.94006L11.54 9.96006" stroke="currentColor" strokeLinecap="round" strokeLinejoin="round" />
            <path d="M4.45996 9.96006L7.03996 5.94006" stroke="currentColor" strokeLinecap="round" strokeLinejoin="round" />
          </svg>
        )
      default:
        return null
    }
  }

  function getProperCapitalization(text) {
    const specialCases = {
      "ai/ml": "AI/ML",
      clickpipes: "ClickPipes",
      "data ingestion": "Data ingestion",
      "data integration": "Data integration",
      "data management": "Data management",
      "data visualization": "Data visualization",
      "deployment method": "Deployment method",
      "language client": "Language client",
      "security governance": "Security governance",
      "sql client": "SQL client"
    }

    const lowerText = text.toLowerCase()
    if (specialCases[lowerText]) {
      return specialCases[lowerText]
    }

    return text
      .split(" ")
      .map((word) => word.charAt(0).toUpperCase() + word.slice(1))
      .join(" ")
  }

  function getSectionDescription(type) {
    const descriptions = {
      ClickPipes: "ClickPipes é um mecanismo de integração que torna a ingestão de grandes volumes de dados de diversas fontes tão simples quanto clicar em alguns botões.",
      "Data ingestion": "Simplifique seus pipelines de dados com o ClickHouse! Integrações perfeitas garantem uma ingestão eficiente, otimizando a análise em tempo real.",
      "Data visualization": "Dê vida às suas histórias de dados! As integrações do ClickHouse aprimoram a visualização, tornando os insights mais claros e acionáveis.",
      "SQL client": "Acesse e consulte bancos de dados ClickHouse usando ferramentas e interfaces de SQL client familiares.",
      "Language client": "Programe com conforto! As integrações de language client do ClickHouse tornam o acesso a dados fluente em diversas linguagens de programação.",
      "AI/ML": "Aproveite o ClickHouse para cargas de trabalho de machine learning e IA com ferramentas e frameworks de ML integrados.",
      "Data management": "Gerencie, monitore e otimize seus dados no ClickHouse com ferramentas especializadas de gerenciamento.",
      "Data integration": "Integre o ClickHouse à sua infraestrutura de dados e fluxos de trabalho existentes.",
      "Security governance": "Implemente frameworks de segurança e governança para o seu ambiente ClickHouse."
    }
    return descriptions[type] || "Integre o ClickHouse com ferramentas e serviços especializados."
  }

  // Plain render function (not a component) so cards reconcile by key instead of
  // remounting on every IntegrationGrid render. isDark is passed in from the single
  // top-level useDarkMode() call.
  function renderIntegrationCard(integration, isDark, key) {
    const getNavigationLink = (docsLink, slug) => {
      if (!docsLink) {
        return slug
      }

      const clickhouseDocsMatch = docsLink.match(/https:\/\/clickhouse\.com\/docs\/(.+)/)
      if (clickhouseDocsMatch) {
        return `/${clickhouseDocsMatch[1]}`
      }

      return docsLink
    }

    const getLogoSrc = () => {
      if (!isDark && integration.integration_logo_dark) {
        return integration.integration_logo_dark
      }
      return integration.integration_logo
    }

    const linkTo = getNavigationLink(integration.docsLink, integration.slug)

    // External links point outside the docs (not to clickhouse.com/docs)
    const isExternalLink = linkTo.startsWith("http") && !linkTo.includes("clickhouse.com/docs")

    return (
      <a key={key} href={linkTo} className="block no-underline integration-card-link" style={{ textDecoration: "none" }} {...(isExternalLink ? { target: "_blank", rel: "noopener noreferrer" } : {})}>
        <div
          className="relative flex min-h-[120px] flex-col items-center justify-center rounded-xl transition-all duration-300 p-5 cursor-pointer integration-card"
          style={{
            aspectRatio: "1 / 1",
            backgroundColor: isDark ? "rgba(65, 65, 65, 0.5)" : "#fff",
            border: isDark ? "1px solid rgba(255, 255, 255, 0.08)" : "1px solid #e0e0e0",
            boxShadow: isDark ? "0 1px 3px 0 rgba(0, 0, 0, 0.2)" : "0 1px 3px 0 rgba(0, 0, 0, 0.1)"
          }}
        >
          {isExternalLink && (
            <div className="integration-external-overlay">
              <svg fill="none" stroke="currentColor" viewBox="0 0 24 24" xmlns="http://www.w3.org/2000/svg">
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M10 6H6a2 2 0 00-2 2v10a2 2 0 002 2h10a2 2 0 002-2v-4M14 4h6m0 0v6m0-6L10 14" />
              </svg>
            </div>
          )}
          {integration.integration_tier && integration.integration_tier !== "community" && <div className="absolute top-3 right-3 opacity-70">{getTierIcon(integration.integration_tier)}</div>}
          <div className="w-full flex flex-col items-center justify-center gap-2">
            <div className="w-full flex items-center justify-center">
              <img src={getLogoSrc()} alt={`logo de ${integration.integration_title || integration.slug}`} className="object-contain" style={{ width: "64px", height: "64px", pointerEvents: "none" }} />
            </div>
            <div className="w-full text-center text-sm font-semibold" style={{ color: "#000" }}>
              {integration.integration_title}
            </div>
          </div>
        </div>
      </a>
    )
  }

  function renderIntegrationCards(integrations, isDark) {
    return (
      <div className="grid grid-cols-2 md:grid-cols-3 lg:grid-cols-4 xl:grid-cols-5 2xl:grid-cols-6 gap-6 my-8">
        {integrations.map((integration, index) => renderIntegrationCard(integration, isDark, `${integration.slug}-${integration.integration_title || index}`))}
      </div>
    )
  }

  function transformCMSData(cmsData) {
    const categoryMapping = {
      AI_ML: "AI/ML",
      CLICKPIPES: "ClickPipes",
      DATA_INGESTION: "Data ingestion",
      DATA_INTEGRATION: "Data integration",
      DATA_MANAGEMENT: "Data management",
      DATA_VISUALIZATION: "Data visualization",
      LANGUAGE_CLIENT: "Language client",
      SECURITY_GOVERNANCE: "Security governance",
      SQL_CLIENT: "SQL client"
    }

    return cmsData.map((item) => {
      const integrationTypes = item.category ? [categoryMapping[item.category] || item.category] : []
      const integrationTier = item.supportLevel?.toLowerCase() || ""

      return {
        slug: item.slug.startsWith("/") ? item.slug : `/${item.slug}`,
        docsLink: item.docsLink,
        integration_logo: item.logo?.url ? `https://clickhouse.com${item.logo.url}` : "",
        integration_logo_dark: item.logo_dark?.url ? `https://clickhouse.com${item.logo_dark.url}` : undefined,
        integration_type: integrationTypes,
        integration_title: item.name,
        integration_tier: integrationTier
      }
    })
  }

  function useCMSIntegrations() {
    // Mintlify remounts the whole content subtree on a theme toggle, which would
    // otherwise re-show the "Loading…" state and refetch. Seed state from a cross-remount
    // cache on window so cards render instantly; the effect still refreshes in the background.
    const cached = (typeof window !== "undefined" && window.__chIntegrationsCache) || null
    const [integrations, setIntegrations] = useState(cached || [])
    const [loading, setLoading] = useState(!cached)
    const [error, setError] = useState(null)

    useEffect(() => {
      const cacheIntegrations = (data) => {
        if (typeof window !== "undefined") window.__chIntegrationsCache = data
      }
      const fetchIntegrations = async () => {
        try {
          const controller = new AbortController()
          const timeoutId = setTimeout(() => {
            controller.abort()
            console.log("Requisição ao CMS expirou após 8 segundos")
          }, 8000)

          const response = await fetch(
            "https://staging-cms.clickhouse.com/api/integrations?fields[0]=name&fields[1]=slug&fields[2]=category&fields[3]=supportLevel&fields[4]=docsLink&populate[logo][fields][0]=url&populate[logo_dark][fields][0]=url&pagination[pageSize]=500",
            {
              signal: controller.signal,
              headers: {
                Accept: "application/json"
              }
            }
          )

          clearTimeout(timeoutId)

          if (!response.ok) {
            throw new Error(`Erro HTTP! status: ${response.status}`)
          }

          const data = await response.json()
          const transformedData = transformCMSData(data.data || [])

          setIntegrations(transformedData)
          cacheIntegrations(transformedData)
          setError(null)
          console.log("Atualizado com sucesso com dados atualizados do CMS")
        } catch (cmsErr) {
          if (cmsErr instanceof Error) {
            if (cmsErr.name === "AbortError") {
              console.log("A requisição ao CMS foi cancelada por timeout")
            } else {
              console.error("Erro ao carregar integrações do CMS:", cmsErr.message)
            }
          }

          if (integrations.length === 0) {
            setError("Não foi possível carregar as integrações. Tente recarregar a página.")
          }
        } finally {
          setLoading(false)
        }
      }

      fetchIntegrations()
    }, [])

    return { integrations, loading, error }
  }

  const { integrations, loading, error } = useCMSIntegrations()

  // Detect dark mode once for the whole grid (single observer) and pass down to cards.
  const isDark = useDarkMode()

  const [searchTerm, setSearchTerm] = useState(() => {
    if (typeof window !== "undefined") {
      return localStorage.getItem("integrations-search") || ""
    }
    return ""
  })

  const [selectedFilter, setSelectedFilter] = useState(() => {
    if (typeof window !== "undefined") {
      return localStorage.getItem("integrations-filter") || "All"
    }
    return "All"
  })

  const [selectedTier, setSelectedTier] = useState(() => {
    if (typeof window !== "undefined") {
      return localStorage.getItem("integrations-tier") || "All"
    }
    return "All"
  })

  useEffect(() => {
    if (typeof window !== "undefined") {
      localStorage.setItem("integrations-search", searchTerm)
    }
  }, [searchTerm])

  useEffect(() => {
    if (typeof window !== "undefined") {
      localStorage.setItem("integrations-filter", selectedFilter)
    }
  }, [selectedFilter])

  useEffect(() => {
    if (typeof window !== "undefined") {
      localStorage.setItem("integrations-tier", selectedTier)
    }
  }, [selectedTier])

  const integrationTypes = useMemo(() => {
    const types = new Set()
    integrations.forEach((integration) => {
      integration.integration_type.forEach((type) => {
        types.add(type)
      })
    })

    const sortOrder = ["Language client", "ClickPipes", "Data ingestion", "Data visualization", "AI/ML", "Data integration", "Data management", "Security governance", "SQL client"]

    return Array.from(types).sort((a, b) => {
      const indexA = sortOrder.indexOf(a)
      const indexB = sortOrder.indexOf(b)

      if (indexA !== -1 && indexB !== -1) {
        return indexA - indexB
      }

      if (indexA !== -1) return -1
      if (indexB !== -1) return 1

      return a.localeCompare(b)
    })
  }, [integrations])

  const integrationTiers = useMemo(() => {
    const tiers = new Set()
    integrations.forEach((integration) => {
      if (integration.integration_tier) {
        tiers.add(integration.integration_tier)
      }
    })

    const tierSortOrder = ["core", "partner", "community"]

    return Array.from(tiers).sort((a, b) => {
      const indexA = tierSortOrder.indexOf(a)
      const indexB = tierSortOrder.indexOf(b)

      if (indexA !== -1 && indexB !== -1) {
        return indexA - indexB
      }

      if (indexA !== -1) return -1
      if (indexB !== -1) return 1

      return a.localeCompare(b)
    })
  }, [integrations])

  const filteredIntegrations = useMemo(() => {
    const filtered = integrations.filter((integration) => {
      const matchesSearch = integration.integration_title?.toLowerCase().includes(searchTerm.toLowerCase()) || integration.slug.toLowerCase().includes(searchTerm.toLowerCase())

      const matchesFilter = selectedFilter === "All" || integration.integration_type.some((type) => type === selectedFilter)

      const matchesTier = selectedTier === "All" || integration.integration_tier === selectedTier

      return matchesSearch && matchesFilter && matchesTier
    })

    return filtered.sort((a, b) => {
      const tierOrder = ["core", "partner", "community", ""]
      const tierA = a.integration_tier || ""
      const tierB = b.integration_tier || ""

      const tierIndexA = tierOrder.indexOf(tierA)
      const tierIndexB = tierOrder.indexOf(tierB)

      if (tierIndexA !== tierIndexB) {
        return tierIndexA - tierIndexB
      }

      return (a.integration_title || "").localeCompare(b.integration_title || "")
    })
  }, [integrations, searchTerm, selectedFilter, selectedTier])

  const groupedIntegrations = useMemo(() => {
    const grouped = new Map()

    filteredIntegrations.forEach((integration) => {
      integration.integration_type.forEach((type) => {
        if (!grouped.has(type)) {
          grouped.set(type, [])
        }
        if (!grouped.get(type)?.find((item) => item.slug === integration.slug && item.integration_title === integration.integration_title)) {
          grouped.get(type)?.push(integration)
        }
      })
    })

    grouped.forEach((integrationsArray) => {
      integrationsArray.sort((a, b) => {
        const tierOrder = ["core", "partner", "community", ""]
        const tierA = a.integration_tier || ""
        const tierB = b.integration_tier || ""

        const tierIndexA = tierOrder.indexOf(tierA)
        const tierIndexB = tierOrder.indexOf(tierB)

        if (tierIndexA !== tierIndexB) {
          return tierIndexA - tierIndexB
        }

        return (a.integration_title || "").localeCompare(b.integration_title || "")
      })
    })

    return grouped
  }, [filteredIntegrations])

  if (loading) {
    return (
      <div className="text-center py-12">
        <p className="text-gray-600 dark:text-gray-400">Carregando integrações...</p>
      </div>
    )
  }

  if (error) {
    return (
      <div className="text-center py-12">
        <p className="text-red-600 dark:text-red-400">Falha ao carregar integrações: {error}</p>
        <p className="text-gray-600 dark:text-gray-400">Tente recarregar a página.</p>
      </div>
    )
  }

  if (integrations.length === 0) {
    return (
      <div className="text-center py-12">
        <p className="text-gray-600 dark:text-gray-400">Nenhuma integração encontrada.</p>
      </div>
    )
  }

  return (
    <>
      <style
        dangerouslySetInnerHTML={{
          __html: `
        .integration-card {
          background-color: #fff !important;
        }
        .dark .integration-card {
          background-color: rgba(65, 65, 65, 0.5) !important;
        }
        .dark .integration-card div.text-sm {
          color: #fff !important;
        }
        .dark .integration-card div {
          color: #fff !important;
        }
        .integration-card:hover {
          transform: translateY(-2px);
          box-shadow: 0 4px 12px 0 rgba(0, 0, 0, 0.15);
          border-color: rgba(252, 255, 116, 0.3) !important;
        }
        .dark .integration-card:hover {
          box-shadow: 0 4px 12px 0 rgba(0, 0, 0, 0.3);
          border-color: rgba(252, 255, 116, 0.3) !important;
        }
        /* Keep card links free of the prose link color/underline in both themes and on
           hover. The light-mode selector mirrors Mintlify's html:not(.dark) .prose a... rule
           (which carries an extra element in its specificity) plus the .integration-card-link
           class, so it must outrank it; same for the dark variants. */
        html:not(.dark) .prose a.integration-card-link:not(.card):not(.card *),
        html:not(.dark) .prose a.integration-card-link:not(.card):not(.card *):hover,
        .dark .prose a.integration-card-link:not(.card):not(.card *),
        .dark .prose a.integration-card-link:not(.card):not(.card *):hover,
        :is(.dark) .prose a.integration-card-link:not(.card):not(.card *),
        :is(.dark) .prose a.integration-card-link:not(.card):not(.card *):hover {
          text-decoration: none !important;
          color: inherit !important;
        }
        /* External link overlay: hidden until card hover */
        .integration-external-overlay {
          position: absolute;
          top: 0;
          left: 0;
          right: 0;
          bottom: 0;
          display: flex;
          align-items: center;
          justify-content: center;
          background: rgba(0, 0, 0, 0.15);
          backdrop-filter: blur(4px);
          -webkit-backdrop-filter: blur(4px);
          border-radius: 0.75rem;
          opacity: 0;
          transition: opacity 0.2s ease;
          pointer-events: none;
          z-index: 1;
        }
        .dark .integration-external-overlay {
          background: rgba(255, 255, 255, 0.15);
        }
        .integration-external-overlay svg {
          width: 32px;
          height: 32px;
          color: #fff;
        }
        .dark .integration-external-overlay svg {
          color: #1f1f1f;
        }
        .integration-card:hover .integration-external-overlay {
          opacity: 1;
        }
      `
        }}
      />
      <div className="max-w-7xl mx-auto px-4">
        <div className="my-8 flex justify-center items-center" style={{ margin: "2rem 0 12px 0" }}>
          <div className="relative w-full" style={{ maxWidth: "500px" }}>
            <svg
              className="absolute pointer-events-none z-10"
              style={{ left: "12px", top: "50%", transform: "translateY(-50%)", width: "14px", height: "14px", color: "#666" }}
              fill="none"
              stroke="currentColor"
              viewBox="0 0 24 24"
            >
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M21 21l-6-6m2-5a7 7 0 11-14 0 7 7 0 0114 0z" />
            </svg>
            <input
              type="text"
              placeholder="Buscar por integração"
              value={searchTerm}
              onChange={(e) => setSearchTerm(e.target.value)}
              className="w-full text-sm border rounded-xl focus:outline-none bg-[#F6F7FA] dark:bg-[#282828] text-black dark:text-white border-gray-300 dark:border-gray-600 focus:border-[#FAFF69]"
              style={{
                height: "38px",
                padding: "0.5rem 0.75rem 0.5rem 2.5rem",
                lineHeight: "1.4",
                boxSizing: "border-box"
              }}
            />
          </div>
        </div>

        <div className="flex flex-col my-8" style={{ gap: "12px" }}>
          <div className="flex flex-wrap justify-center" style={{ gap: "8px" }}>
            <button
              className={`text-sm font-medium rounded-full transition-all cursor-pointer flex items-center justify-center whitespace-nowrap border ${
                selectedFilter === "All"
                  ? "bg-black dark:bg-[#FAFF69] text-white dark:text-black border-black dark:border-[#FAFF69]"
                  : "bg-white dark:bg-[#282828] text-black dark:text-white border-gray-300 dark:border-white/20 hover:border-[#FAFF69] hover:bg-gray-50 dark:hover:bg-[#FAFF69] dark:hover:text-black"
              }`}
              style={{ padding: "6px 12px" }}
              onClick={() => setSelectedFilter("All")}
            >
              Todos
            </button>
            {integrationTypes.map((type) => (
              <button
                key={type}
                className={`text-sm font-medium rounded-full transition-all cursor-pointer flex items-center justify-center whitespace-nowrap border ${
                  selectedFilter === type
                    ? "bg-black dark:bg-[#FAFF69] text-white dark:text-black border-black dark:border-[#FAFF69]"
                    : "bg-white dark:bg-[#282828] text-black dark:text-white border-gray-300 dark:border-white/20 hover:border-[#FAFF69] hover:bg-gray-50 dark:hover:bg-[#FAFF69] dark:hover:text-black"
                }`}
                style={{ padding: "6px 12px" }}
                onClick={() => setSelectedFilter(type)}
              >
                {getProperCapitalization(type)}
              </button>
            ))}
          </div>

          <div className="flex flex-wrap justify-center" style={{ gap: "8px" }}>
            <button
              className={`text-sm font-medium rounded-full transition-all cursor-pointer flex items-center justify-center whitespace-nowrap border ${
                selectedTier === "All"
                  ? "bg-black dark:bg-[#FAFF69] text-white dark:text-black border-black dark:border-[#FAFF69]"
                  : "bg-white dark:bg-[#282828] text-black dark:text-white border-gray-300 dark:border-white/20 hover:border-[#FAFF69] hover:bg-gray-50 dark:hover:bg-[#FAFF69] dark:hover:text-black"
              }`}
              style={{ padding: "6px 12px" }}
              onClick={() => setSelectedTier("All")}
            >
              Todos os níveis
            </button>
            {integrationTiers.map((tier) => (
              <button
                key={tier}
                className={`text-sm font-medium rounded-full transition-all cursor-pointer flex items-center justify-center whitespace-nowrap border ${
                  selectedTier === tier
                    ? "bg-black dark:bg-[#FAFF69] text-white dark:text-black border-black dark:border-[#FAFF69]"
                    : "bg-white dark:bg-[#282828] text-black dark:text-white border-gray-300 dark:border-white/20 hover:border-[#FAFF69] hover:bg-gray-50 dark:hover:bg-[#FAFF69] dark:hover:text-black"
                }`}
                style={{ padding: "6px 12px", gap: "6px" }}
                onClick={() => setSelectedTier(tier)}
              >
                {getTierIcon(tier, true)}
                {tier.charAt(0).toUpperCase() + tier.slice(1)}
              </button>
            ))}
          </div>
        </div>

        {selectedFilter === "All" ? (
          Array.from(groupedIntegrations.entries())
            .sort(([a], [b]) => {
              const sortOrder = ["Language client", "ClickPipes", "Data ingestion", "Data visualization", "AI/ML", "Data integration", "Data management", "Security governance", "SQL client"]

              const indexA = sortOrder.indexOf(a)
              const indexB = sortOrder.indexOf(b)

              if (indexA !== -1 && indexB !== -1) {
                return indexA - indexB
              }

              if (indexA !== -1) return -1
              if (indexB !== -1) return 1

              return a.localeCompare(b)
            })
            .map(([type, typeIntegrations]) => (
              <section key={type} style={{ margin: "3rem 0" }}>
                <h2 className="text-2xl font-bold mb-2 text-black dark:text-white" style={{ marginBottom: "0.5rem" }}>
                  {getProperCapitalization(type)}
                </h2>
                <p className="text-base text-gray-600 dark:text-gray-400 mb-8 leading-relaxed" style={{ marginBottom: "2rem", lineHeight: "1.6" }}>
                  {getSectionDescription(type)}
                </p>
                {renderIntegrationCards(typeIntegrations, isDark)}
              </section>
            ))
        ) : (
          <section style={{ margin: "3rem 0" }}>
            <h2 className="text-2xl font-bold mb-2 text-black dark:text-white" style={{ marginBottom: "0.5rem" }}>
              {getProperCapitalization(selectedFilter)}
            </h2>
            <p className="text-base text-gray-600 dark:text-gray-400 mb-8 leading-relaxed" style={{ marginBottom: "2rem", lineHeight: "1.6" }}>
              {getSectionDescription(selectedFilter)}
            </p>
            {renderIntegrationCards(filteredIntegrations, isDark)}
          </section>
        )}

        {filteredIntegrations.length === 0 && (
          <div className="text-center py-12">
            <p className="text-gray-600 dark:text-gray-400 text-lg">Nenhuma integração encontrada para os critérios informados.</p>
          </div>
        )}
      </div>
    </>
  )
}
