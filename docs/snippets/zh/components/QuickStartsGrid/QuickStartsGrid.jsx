export const QuickStartsGrid = ({ quickStartsData = [], featured = [] }) => {
  const featuredIds = featured.map((f) => f.id)
  const data = quickStartsData || []
  const assetBase = typeof window !== "undefined" && window.location.pathname.startsWith("/docs") ? "/docs" : ""
  const withBase = (p) => (p && p.startsWith("/") ? assetBase + p : p)

  // Safely read a persisted string array from localStorage. A corrupted or
  // hand-edited value must never throw out of a useState initializer, which
  // would crash the whole page render — fall back to the default instead.
  const readStoredList = (key, fallback) => {
    if (typeof window === "undefined") return fallback
    try {
      const raw = localStorage.getItem(key)
      if (!raw) return fallback
      const parsed = JSON.parse(raw)
      return Array.isArray(parsed) ? parsed : fallback
    } catch {
      return fallback
    }
  }

  // State management with localStorage
  const [searchTerm, setSearchTerm] = useState(() => {
    if (typeof window !== "undefined") {
      return localStorage.getItem("quickstarts-search") || ""
    }
    return ""
  })

  const [selectedUseCases, setSelectedUseCases] = useState(() => readStoredList("quickstarts-usecases", ["All"]))

  const [selectedProducts, setSelectedProducts] = useState(() => readStoredList("quickstarts-products", ["All"]))

  const [currentPage, setCurrentPage] = useState(1)
  const itemsPerPage = 6

  const [showFilters, setShowFilters] = useState(() => {
    if (typeof window !== "undefined") {
      return localStorage.getItem("quickstarts-show-filters") !== "false"
    }
    return true
  })

  // Track the lg breakpoint so the drawer can collapse left (desktop) or up
  // (mobile). Inline styles can't be responsive, so we branch on this in JS.
  const [isDesktop, setIsDesktop] = useState(true)
  useEffect(() => {
    if (typeof window === "undefined" || !window.matchMedia) return
    const mq = window.matchMedia("(min-width: 1024px)")
    const update = () => setIsDesktop(mq.matches)
    update()
    mq.addEventListener("change", update)
    return () => mq.removeEventListener("change", update)
  }, [])

  // Persist to localStorage
  useEffect(() => {
    if (typeof window !== "undefined") localStorage.setItem("quickstarts-search", searchTerm)
  }, [searchTerm])

  useEffect(() => {
    if (typeof window !== "undefined") localStorage.setItem("quickstarts-usecases", JSON.stringify(selectedUseCases))
  }, [selectedUseCases])

  useEffect(() => {
    if (typeof window !== "undefined") localStorage.setItem("quickstarts-products", JSON.stringify(selectedProducts))
  }, [selectedProducts])

  useEffect(() => {
    if (typeof window !== "undefined") localStorage.setItem("quickstarts-show-filters", String(showFilters))
  }, [showFilters])

  // Reset page when filters change
  useEffect(() => {
    setCurrentPage(1)
  }, [searchTerm, selectedUseCases, selectedProducts])

  // Generic multi-select toggle: clicking "All" clears others; empty -> ['All'].
  const makeToggle = (setter) => (value) => {
    setter((prev) => {
      if (value === "All") return ["All"]
      const withoutAll = prev.filter((v) => v !== "All")
      const result = withoutAll.includes(value) ? withoutAll.filter((v) => v !== value) : [...withoutAll, value]
      return result.length === 0 ? ["All"] : result
    })
  }

  const toggleUseCase = makeToggle(setSelectedUseCases)
  const toggleProduct = makeToggle(setSelectedProducts)

  const useCaseOptions = ["全部", "实时分析", "数据仓库", "可观测性", "AI/ML"]
  const productOptions = ["全部", "自管理", "Cloud", "ClickPipes", "语言客户端", "ClickStack", "chDB"]

  const resetFilters = () => {
    setSearchTerm("")
    setSelectedUseCases(["All"])
    setSelectedProducts(["All"])
  }

  const hasActiveFilters = searchTerm !== "" || !selectedUseCases.includes("All") || !selectedProducts.includes("All")

  // Filtering logic
  const filteredQuickStarts = useMemo(() => {
    const term = searchTerm.toLowerCase()
    return data.filter((quickStart) => {
      // Exclude featured quickstarts from the explore section
      if (featuredIds.includes(quickStart.id)) return false

      const matchesSearch = term === "" || quickStart.title.toLowerCase().includes(term) || (quickStart.description || "").toLowerCase().includes(term)

      // "All" in the selection means no filter. Otherwise a quickstart matches
      // only if every one of its tags is within the selection — so selecting
      // "Data warehousing" excludes quickstarts also tagged with other use
      // cases (and generic "All"-tagged ones).
      const useCases = quickStart.useCases || []
      const matchesUseCases = selectedUseCases.includes("All") || (useCases.length > 0 && useCases.every((uc) => selectedUseCases.includes(uc)))

      const products = quickStart.products || []
      const matchesProducts = selectedProducts.includes("All") || (products.length > 0 && products.every((p) => selectedProducts.includes(p)))

      return matchesSearch && matchesUseCases && matchesProducts
    })
  }, [data, searchTerm, selectedUseCases, selectedProducts, featuredIds])

  // Force a full page navigation so the quickstart opens scrolled to the top,
  // instead of Mintlify's client-side routing keeping the explorer scroll
  // position. Modifier/middle clicks fall through to default (open in new tab).
  const handleCardClick = (e, href) => {
    if (e.defaultPrevented) return
    if (e.metaKey || e.ctrlKey || e.shiftKey || e.altKey || e.button !== 0) return
    e.preventDefault()
    window.location.assign(withBase(href))
  }

  // Featured quickstarts, in the order listed in `featured`. Each keeps its banner
  // image; the href falls back to the conventional quickstart path if the data
  // hasn't loaded yet so the cards are clickable immediately.
  const featuredQuickStarts = featured
    .map((f) => {
      const qs = data.find((q) => q.id === f.id)
      return {
        id: f.id,
        image: f.image,
        href: (qs && qs.href) || `/zh/get-started/quickstarts/${f.id}`,
        title: (qs && qs.title) || ""
      }
    })
    .filter((f) => f.image)

  // Always-visible filter group (not collapsible)
  const FilterGroup = ({ label, options, selectedOptions, onToggle }) => {
    const activeCount = selectedOptions.filter((o) => o !== "All").length
    const displayLabel = activeCount > 0 ? `${label} (${activeCount})` : label

    return (
      <div style={{ minWidth: "160px" }}>
        <div className="text-sm font-semibold text-black dark:text-white" style={{ padding: "4px 0" }}>
          {displayLabel}
        </div>
        <div className="mt-1">
          {options.map((option) => (
            <label
              key={option}
              className="flex items-center gap-2 py-1.5 cursor-pointer transition-colors"
              onClick={(e) => {
                e.preventDefault()
                onToggle(option)
              }}
            >
              <span
                className="flex items-center justify-center w-4 h-4 rounded border flex-shrink-0"
                style={{
                  borderColor: selectedOptions.includes(option) ? "#FAFF69" : "rgba(156, 163, 175, 0.6)",
                  backgroundColor: selectedOptions.includes(option) ? "#FAFF69" : "transparent"
                }}
              >
                {selectedOptions.includes(option) && (
                  <svg width="10" height="10" viewBox="0 0 10 10" fill="none" xmlns="http://www.w3.org/2000/svg">
                    <path d="M2 5L4 7L8 3" stroke="black" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round" />
                  </svg>
                )}
              </span>
              <span className="text-sm text-black dark:text-white">{option}</span>
            </label>
          ))}
        </div>
      </div>
    )
  }

  return (
    <>
      <div style={{ maxWidth: "1312px", marginLeft: "max(0px, calc((100vw - 1312px) / 2 - 19rem))", marginRight: "auto", paddingLeft: "1.75rem", paddingRight: "1.75rem" }}>
        <div className="my-8">
          {/* 精选快速入门 section - full width banner image cards */}
          {featuredQuickStarts.length > 0 && (
            <div className="mb-12">
              <h2 className="text-2xl font-semibold text-gray-900 dark:text-zinc-50 mb-6">精选快速入门</h2>
              <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6">
                {featuredQuickStarts.map((quickStart) => (
                  <a
                    key={quickStart.id}
                    href={quickStart.href}
                    onClick={(e) => handleCardClick(e, quickStart.href)}
                    className="group block rounded-xl overflow-hidden border border-gray-200 dark:border-white/10 transition-all hover:border-black dark:hover:border-[#FAFF69] hover:shadow-md"
                  >
                    {/* Rendered as a background image (not <img>) so Mintlify's
                        click-to-zoom doesn't hijack the link navigation. */}
                    <div
                      role="img"
                      aria-label={quickStart.title}
                      className="w-full aspect-[16/9] bg-cover bg-center transition-transform duration-200 group-hover:scale-[1.02]"
                      style={{ backgroundImage: `url(${withBase(quickStart.image)})` }}
                    />
                  </a>
                ))}
              </div>
            </div>
          )}

          {/* Explore section with sidebar */}
          <div className="flex flex-col lg:flex-row gap-8">
            {/* Filter rail - collapses left on desktop, up on mobile; a toggle sits on the divider */}
            <div className={isDesktop ? "flex-shrink-0 transition-[width] duration-300 ease-in-out" : "w-full"} style={isDesktop ? { width: showFilters ? "16rem" : "0px" } : undefined}>
              <div className="lg:sticky relative" style={isDesktop ? { top: "8.5rem" } : undefined}>
                {/* Divider line: vertical on the panel's right (desktop), horizontal below it (mobile) */}
                <div
                  aria-hidden="true"
                  className="absolute bg-gray-200 dark:bg-white/10"
                  style={isDesktop ? { left: "100%", top: 0, bottom: 0, width: "1px" } : { top: "100%", left: 0, right: 0, height: "1px" }}
                />
                {/* Toggle button, centered on the divider line */}
                <button
                  onClick={() => setShowFilters((prev) => !prev)}
                  aria-label={showFilters ? "隐藏筛选器" : "显示筛选器"}
                  title={showFilters ? "隐藏筛选器" : "显示筛选器"}
                  className="flex items-center justify-center absolute z-20 cursor-pointer rounded-full border transition-colors border-gray-300 dark:border-white/20 hover:border-black dark:hover:border-[#FAFF69] bg-white dark:bg-[#1B1B18] text-gray-500 dark:text-gray-400 hover:text-black dark:hover:text-[#FAFF69] shadow-sm"
                  style={
                    isDesktop
                      ? { left: "100%", top: "50%", width: "28px", height: "28px", transform: "translate(-50%, -50%)" }
                      : { top: "100%", left: "50%", width: "28px", height: "28px", transform: "translate(-50%, -50%)" }
                  }
                >
                  <svg
                    width="12"
                    height="12"
                    viewBox="0 0 12 12"
                    fill="none"
                    xmlns="http://www.w3.org/2000/svg"
                    className="transition-transform duration-300"
                    style={{ transform: `rotate(${isDesktop ? (showFilters ? 0 : 180) : showFilters ? 90 : -90}deg)` }}
                  >
                    <path d="M7.5 2.5L4 6L7.5 9.5" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round" />
                  </svg>
                </button>

                {/* Panel body: collapses by width (desktop) or max-height (mobile); overflow-hidden clips it.
                    The trailing padding keeps the controls off the divider line. */}
                <div
                  className="overflow-hidden transition-all duration-300"
                  style={
                    isDesktop
                      ? { width: "16rem", paddingRight: "1.5rem", opacity: showFilters ? 1 : 0, pointerEvents: showFilters ? "auto" : "none" }
                      : {
                          width: "100%",
                          maxHeight: showFilters ? "1500px" : "0px",
                          paddingBottom: showFilters ? "1.5rem" : "0px",
                          opacity: showFilters ? 1 : 0,
                          pointerEvents: showFilters ? "auto" : "none"
                        }
                  }
                >
                  <div className="space-y-6">
                    {/* Search input */}
                    <div>
                      <label className="block text-sm font-semibold text-gray-900 dark:text-zinc-50 mb-3">搜索</label>
                      <div className="relative w-full">
                        <svg
                          className="absolute pointer-events-none z-10"
                          style={{ left: "12px", top: "50%", transform: "translateY(-50%)", width: "16px", height: "16px", color: "#666" }}
                          fill="none"
                          stroke="currentColor"
                          viewBox="0 0 24 24"
                        >
                          <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M21 21l-6-6m2-5a7 7 0 11-14 0 7 7 0 0114 0z" />
                        </svg>
                        <input
                          type="text"
                          placeholder="搜索快速入门..."
                          value={searchTerm}
                          onChange={(e) => setSearchTerm(e.target.value)}
                          className="w-full text-sm border rounded-xl focus:outline-none bg-white dark:bg-[#1B1B18] text-black dark:text-white border-gray-300 dark:border-gray-600 focus:border-black dark:focus:border-[#FAFF69]"
                          style={{ height: "42px", padding: "0.5rem 0.75rem 0.5rem 2.75rem", lineHeight: "1.4", boxSizing: "border-box" }}
                        />
                      </div>
                    </div>

                    {/* Filters */}
                    <div>
                      <div className="space-y-5">
                        <FilterGroup label="用例" options={useCaseOptions} selectedOptions={selectedUseCases} onToggle={toggleUseCase} />
                        <FilterGroup label="产品领域" options={productOptions} selectedOptions={selectedProducts} onToggle={toggleProduct} />
                      </div>
                    </div>

                    {/* Reset button */}
                    {hasActiveFilters && (
                      <button
                        onClick={resetFilters}
                        className="w-full text-sm font-medium px-4 py-2 rounded-lg transition-all cursor-pointer border border-gray-300 dark:border-white/20 hover:border-black dark:hover:border-[#FAFF69] bg-white dark:bg-[#1B1B18] text-black dark:text-white"
                      >
                        重置筛选器
                      </button>
                    )}
                  </div>
                </div>
              </div>
            </div>

            {/* Right content area */}
            <div className="flex-1 min-w-0">
              <h2 className="text-2xl font-semibold text-gray-900 dark:text-zinc-50 mb-6">浏览快速入门</h2>

              {filteredQuickStarts.length > 0 ? (
                <>
                  <div className="flex flex-col gap-2">
                    {filteredQuickStarts.slice((currentPage - 1) * itemsPerPage, currentPage * itemsPerPage).map((quickStart) => (
                      <a
                        key={quickStart.id}
                        href={quickStart.href}
                        onClick={(e) => handleCardClick(e, quickStart.href)}
                        className="group block rounded-lg border px-4 py-3 transition-all border-gray-200 dark:border-white/10 hover:border-black dark:hover:border-[#FAFF69] bg-white dark:bg-[#1B1B18]"
                      >
                        <div className="text-sm font-semibold text-gray-900 dark:text-zinc-50">{quickStart.title}</div>
                        <div className="text-sm text-gray-600 dark:text-gray-400 mt-0.5 line-clamp-2">{quickStart.description}</div>
                      </a>
                    ))}
                  </div>
                  {/* Pagination */}
                  {(() => {
                    const totalPages = Math.ceil(filteredQuickStarts.length / itemsPerPage)
                    if (totalPages <= 1) return null
                    const hasPrev = currentPage > 1
                    const hasNext = currentPage < totalPages
                    return (
                      <div className="flex items-center justify-center gap-3 mt-8">
                        <button
                          onClick={() => hasPrev && setCurrentPage((prev) => prev - 1)}
                          disabled={!hasPrev}
                          className={`p-2 rounded-lg border transition-all ${
                            hasPrev
                              ? "border-gray-300 dark:border-white/20 bg-white dark:bg-[#1B1B18] text-black dark:text-white hover:border-[#FAFF69] cursor-pointer"
                              : "border-gray-200 dark:border-white/10 bg-gray-50 dark:bg-[#1B1B18]/50 text-gray-300 dark:text-white/20 cursor-not-allowed"
                          }`}
                          aria-label="上一页"
                        >
                          <svg width="16" height="16" viewBox="0 0 16 16" fill="none" xmlns="http://www.w3.org/2000/svg">
                            <path d="M10 12L6 8L10 4" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round" />
                          </svg>
                        </button>
                        <span className="text-sm text-gray-600 dark:text-gray-400">
                          第 {currentPage} 页，共 {totalPages} 页
                        </span>
                        <button
                          onClick={() => hasNext && setCurrentPage((prev) => prev + 1)}
                          disabled={!hasNext}
                          className={`p-2 rounded-lg border transition-all ${
                            hasNext
                              ? "border-gray-300 dark:border-white/20 bg-white dark:bg-[#1B1B18] text-black dark:text-white hover:border-[#FAFF69] cursor-pointer"
                              : "border-gray-200 dark:border-white/10 bg-gray-50 dark:bg-[#1B1B18]/50 text-gray-300 dark:text-white/20 cursor-not-allowed"
                          }`}
                          aria-label="下一页"
                        >
                          <svg width="16" height="16" viewBox="0 0 16 16" fill="none" xmlns="http://www.w3.org/2000/svg">
                            <path d="M6 4L10 8L6 12" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round" />
                          </svg>
                        </button>
                      </div>
                    )
                  })()}
                </>
              ) : (
                <div className="text-center py-12 flex flex-col items-center">
                  <p className="text-gray-600 dark:text-gray-400 text-lg block">未找到符合条件的快速入门。</p>
                  <p className="text-gray-500 dark:text-gray-500 text-sm mt-2 block">请尝试调整筛选条件或搜索词。</p>
                </div>
              )}
            </div>
          </div>
        </div>
      </div>
    </>
  )
}