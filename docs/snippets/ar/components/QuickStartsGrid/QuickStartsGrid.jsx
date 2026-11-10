export const QuickStartsGrid = ({ quickStartsData = [], featured = [] }) => {
  const featuredIds = featured.map((f) => f.id)
  const data = quickStartsData || []
  const assetBase = typeof window !== "undefined" && window.location.pathname.startsWith("/docs") ? "/docs" : ""
  const withBase = (p) => (p && p.startsWith("/") ? assetBase + p : p)

  // Filter options. `value` is a stable slug matched against the tag slugs in
  // quickstarts-data.jsx (the generator emits the same slug form), so
  // filtering keeps working when the translation pipeline localizes the
  // labels. Only `label` is display text.
  const useCaseOptions = [
    { value: "real-time-analytics", label: "تحليلات الوقت الفعلي" },
    { value: "data-warehousing", label: "تخزين البيانات" },
    { value: "observability", label: "الأوبزيرفابيليتي" },
    { value: "ai-ml", label: "AI/ML" }
  ]
  const productOptions = [
    { value: "self-managed", label: "ClickHouse (Open-Source)" },
    { value: "cloud", label: "ClickHouse Cloud" },
    { value: "clickpipes", label: "ClickPipes" },
    { value: "language-clients", label: "Language clients" },
    { value: "clickstack", label: "ClickStack" },
    { value: "chdb", label: "chDB" }
  ]

  // Only offer categories that at least one explorable quickstart belongs to
  // (an "all"-tagged quickstart belongs to every use case).
  const explorable = data.filter((qs) => !featuredIds.includes(qs.id))
  const visibleUseCaseOptions = useCaseOptions.filter((o) =>
    explorable.some((qs) => {
      const u = qs.useCases || []
      return u.includes("all") || u.includes(o.value)
    })
  )
  const visibleProductOptions = productOptions.filter((o) => explorable.some((qs) => (qs.products || []).includes(o.value)))

  // All localStorage access goes through these guards. Storage may be absent
  // (SSR) or throw SecurityError (storage-restricted browsers or enterprise
  // policies); persistence is optional, so a failure means "not persisted"
  // rather than a render or effect exception that would take down the page.
  const readStored = (key) => {
    if (typeof window === "undefined") return null
    try {
      return localStorage.getItem(key)
    } catch {
      return null
    }
  }
  const writeStored = (key, value) => {
    if (typeof window === "undefined") return
    try {
      localStorage.setItem(key, value)
    } catch {
      // Storage unavailable — persistence is best-effort, so drop it.
    }
  }

  // Read a persisted selection. A corrupted or hand-edited value must never
  // throw out of a useState initializer, which would crash the whole page
  // render — fall back to the default instead. Values not present in the
  // options (e.g. display strings persisted by an older version of this
  // component) are dropped. An empty selection means no filter.
  const readStoredSelection = (key, options) => {
    const raw = readStored(key)
    if (!raw) return []
    try {
      const parsed = JSON.parse(raw)
      if (!Array.isArray(parsed)) return []
      return parsed.filter((v) => options.some((o) => o.value === v))
    } catch {
      return []
    }
  }

  // State management with localStorage
  const [searchTerm, setSearchTerm] = useState(() => readStored("quickstarts-search") || "")

  const [selectedUseCases, setSelectedUseCases] = useState(() => readStoredSelection("quickstarts-usecases", visibleUseCaseOptions))

  const [selectedProducts, setSelectedProducts] = useState(() => readStoredSelection("quickstarts-products", visibleProductOptions))

  const [currentPage, setCurrentPage] = useState(1)
  const itemsPerPage = 6

  const [showFilters, setShowFilters] = useState(() => readStored("quickstarts-show-filters") !== "false")

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
    writeStored("quickstarts-search", searchTerm)
  }, [searchTerm])

  useEffect(() => {
    writeStored("quickstarts-usecases", JSON.stringify(selectedUseCases))
  }, [selectedUseCases])

  useEffect(() => {
    writeStored("quickstarts-products", JSON.stringify(selectedProducts))
  }, [selectedProducts])

  useEffect(() => {
    writeStored("quickstarts-show-filters", String(showFilters))
  }, [showFilters])

  // Reset page when filters change
  useEffect(() => {
    setCurrentPage(1)
  }, [searchTerm, selectedUseCases, selectedProducts])

  // Generic multi-select toggle; an empty selection means no filter.
  const makeToggle = (setter) => (value) => {
    setter((prev) => (prev.includes(value) ? prev.filter((v) => v !== value) : [...prev, value]))
  }

  const toggleUseCase = makeToggle(setSelectedUseCases)
  const toggleProduct = makeToggle(setSelectedProducts)

  const resetFilters = () => {
    setSearchTerm("")
    setSelectedUseCases([])
    setSelectedProducts([])
  }

  const hasActiveFilters = searchTerm !== "" || selectedUseCases.length > 0 || selectedProducts.length > 0

  // Filtering logic
  const filteredQuickStarts = useMemo(() => {
    const term = searchTerm.toLowerCase()
    return data.filter((quickStart) => {
      // Exclude featured quickstarts from the explore section
      if (featuredIds.includes(quickStart.id)) return false

      const matchesSearch = term === "" || quickStart.title.toLowerCase().includes(term) || (quickStart.description || "").toLowerCase().includes(term)

      // An empty selection means no filter. Otherwise a quickstart matches a
      // group if any of its tags is selected (groups combine with AND). A
      // quickstart tagged "all" applies to every use case, so it matches any
      // use-case selection.
      const useCases = quickStart.useCases || []
      const matchesUseCases = selectedUseCases.length === 0 || useCases.includes("all") || useCases.some((uc) => selectedUseCases.includes(uc))

      const products = quickStart.products || []
      const matchesProducts = selectedProducts.length === 0 || products.some((p) => selectedProducts.includes(p))

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

  // Featured quickstarts, in the order listed in `featured`. The banner art is
  // drawn in code from the title (see below), so no per-locale image is needed.
  // The href falls back to the conventional quickstart path if the data hasn't
  // loaded yet so the cards are clickable immediately.
  const featuredQuickStarts = featured
    .map((f) => {
      const qs = data.find((q) => q.id === f.id)
      return {
        id: f.id,
        href: (qs && qs.href) || `/ar/get-started/quickstarts/${f.id}`,
        title: (qs && qs.title) || ""
      }
    })
    .filter((f) => f.title)

  // Always-visible filter group (not collapsible)
  const FilterGroup = ({ label, options, selectedOptions, onToggle }) => {
    const activeCount = selectedOptions.length
    const displayLabel = activeCount > 0 ? `${label} (${activeCount})` : label

    return (
      <div style={{ minWidth: "160px" }}>
        <div className="text-sm font-semibold text-black dark:text-white" style={{ padding: "4px 0" }}>
          {displayLabel}
        </div>
        <div className="mt-1">
          {options.map((option) => (
            <label
              key={option.value}
              className="flex items-center gap-2 py-1.5 cursor-pointer transition-colors"
              onClick={(e) => {
                e.preventDefault()
                onToggle(option.value)
              }}
            >
              <span
                className="flex items-center justify-center w-4 h-4 rounded border flex-shrink-0"
                style={{
                  borderColor: selectedOptions.includes(option.value) ? "#FAFF69" : "rgba(156, 163, 175, 0.6)",
                  backgroundColor: selectedOptions.includes(option.value) ? "#FAFF69" : "transparent"
                }}
              >
                {selectedOptions.includes(option.value) && (
                  <svg width="10" height="10" viewBox="0 0 10 10" fill="none" xmlns="http://www.w3.org/2000/svg">
                    <path d="M2 5L4 7L8 3" stroke="black" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round" />
                  </svg>
                )}
              </span>
              <span className="text-sm text-black dark:text-white">{option.label}</span>
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
          {/* Featured quickstarts section - full width banner image cards */}
          {featuredQuickStarts.length > 0 && (
            <div className="mb-12">
              <h2 className="text-2xl font-semibold text-gray-900 dark:text-zinc-50 mb-6">البدايات السريعة المميزة</h2>
              <div className="grid grid-cols-1 lg:grid-cols-2 xl:grid-cols-3 gap-6">
                {featuredQuickStarts.map((quickStart) => (
                  <a
                    key={quickStart.id}
                    href={quickStart.href}
                    onClick={(e) => handleCardClick(e, quickStart.href)}
                    className="group block rounded-xl overflow-hidden border border-gray-200 dark:border-white/10 transition-all hover:border-black dark:hover:border-[#FAFF69] hover:shadow-md"
                  >
                    {/* Banner art is drawn in code from the title so it
                        translates automatically — no per-locale PNG needed. */}
                    <div className="relative w-full aspect-[2/1] overflow-hidden bg-[#FAFF69] flex flex-col justify-center px-6 pb-12">
                      <span className="relative z-10 mx-auto max-w-[90%] text-center text-base font-bold leading-tight text-black line-clamp-4">{quickStart.title}</span>
                      <div className="absolute inset-x-0 bottom-0 h-12 bg-[#E7EA5B] flex items-center justify-between px-5">
                        <img src={withBase("/images/clickhouse.svg")} alt="" aria-hidden="true" className="h-[18px] w-auto" style={{ borderRadius: 0, filter: "brightness(0)" }} />
                        <span className="text-sm font-medium text-black">البدء</span>
                      </div>
                    </div>
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
                  aria-label={showFilters ? "إخفاء عوامل التصفية" : "إظهار عوامل التصفية"}
                  title={showFilters ? "إخفاء عوامل التصفية" : "إظهار عوامل التصفية"}
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
                      <label className="block text-sm font-semibold text-gray-900 dark:text-zinc-50 mb-3">بحث</label>
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
                          placeholder="ابحث في البدايات السريعة..."
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
                        <FilterGroup label="حالات الاستخدام" options={visibleUseCaseOptions} selectedOptions={selectedUseCases} onToggle={toggleUseCase} />
                        <FilterGroup label="مجال المنتج" options={visibleProductOptions} selectedOptions={selectedProducts} onToggle={toggleProduct} />
                      </div>
                    </div>

                    {/* Reset button */}
                    {hasActiveFilters && (
                      <button
                        onClick={resetFilters}
                        className="w-full text-sm font-medium px-4 py-2 rounded-lg transition-all cursor-pointer border border-gray-300 dark:border-white/20 hover:border-black dark:hover:border-[#FAFF69] bg-white dark:bg-[#1B1B18] text-black dark:text-white"
                      >
                        إعادة تعيين عوامل التصفية
                      </button>
                    )}
                  </div>
                </div>
              </div>
            </div>

            {/* Right content area */}
            <div className="flex-1 min-w-0">
              <h2 className="text-2xl font-semibold text-gray-900 dark:text-zinc-50 mb-6">استكشاف البدايات السريعة</h2>

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
                          aria-label="الصفحة السابقة"
                        >
                          <svg width="16" height="16" viewBox="0 0 16 16" fill="none" xmlns="http://www.w3.org/2000/svg">
                            <path d="M10 12L6 8L10 4" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round" />
                          </svg>
                        </button>
                        <span className="text-sm text-gray-600 dark:text-gray-400">
                          الصفحة {currentPage} / {totalPages}
                        </span>
                        <button
                          onClick={() => hasNext && setCurrentPage((prev) => prev + 1)}
                          disabled={!hasNext}
                          className={`p-2 rounded-lg border transition-all ${
                            hasNext
                              ? "border-gray-300 dark:border-white/20 bg-white dark:bg-[#1B1B18] text-black dark:text-white hover:border-[#FAFF69] cursor-pointer"
                              : "border-gray-200 dark:border-white/10 bg-gray-50 dark:bg-[#1B1B18]/50 text-gray-300 dark:text-white/20 cursor-not-allowed"
                          }`}
                          aria-label="الصفحة التالية"
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
                  <p className="text-gray-600 dark:text-gray-400 text-lg block">لا توجد أدلة بدء سريع تطابق معاييرك.</p>
                  <p className="text-gray-500 dark:text-gray-500 text-sm mt-2 block">جرّب تعديل عوامل التصفية أو مصطلح البحث.</p>
                </div>
              )}
            </div>
          </div>
        </div>
      </div>
    </>
  )
}