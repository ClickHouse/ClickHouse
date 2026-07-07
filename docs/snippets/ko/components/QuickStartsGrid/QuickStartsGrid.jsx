export const QuickStartsGrid = ({ quickStartsData = [], featured = [] }) => {
  const featuredIds = featured.map((f) => f.id)
  const data = quickStartsData || []
  const assetBase = typeof window !== "undefined" && window.location.pathname.startsWith("/docs") ? "/docs" : ""
  const withBase = (p) => (p && p.startsWith("/") ? assetBase + p : p)

  // localStorage에서 저장된 문자열 배열을 안전하게 읽습니다. 손상되었거나
  // 직접 수정된 값이 useState 초기화 함수 내에서 예외를 발생시키면
  // 페이지 전체 렌더링이 중단될 수 있으므로, 이 경우 기본값으로 폴백합니다.
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

  // localStorage를 활용한 상태 관리
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

  // lg 브레이크포인트를 추적하여 드로어가 왼쪽(데스크톱) 또는 위쪽(모바일)으로
  // 접힐 수 있도록 합니다. 인라인 스타일은 반응형을 지원하지 않으므로 JS에서 분기 처리합니다.
  const [isDesktop, setIsDesktop] = useState(true)
  useEffect(() => {
    if (typeof window === "undefined" || !window.matchMedia) return
    const mq = window.matchMedia("(min-width: 1024px)")
    const update = () => setIsDesktop(mq.matches)
    update()
    mq.addEventListener("change", update)
    return () => mq.removeEventListener("change", update)
  }, [])

  // localStorage에 저장
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

  // 필터 변경 시 페이지 초기화
  useEffect(() => {
    setCurrentPage(1)
  }, [searchTerm, selectedUseCases, selectedProducts])

  // 범용 다중 선택 토글: "All" 클릭 시 나머지 선택 해제, 선택 항목이 없으면 ['All']로 설정.
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

  const useCaseOptions = ["All", "실시간 분석", "데이터 웨어하우징", "관측성", "AI/ML"]
  const productOptions = ["All", "자가 관리형", "Cloud", "ClickPipes", "언어 클라이언트", "ClickStack", "chDB"]

  const resetFilters = () => {
    setSearchTerm("")
    setSelectedUseCases(["All"])
    setSelectedProducts(["All"])
  }

  const hasActiveFilters = searchTerm !== "" || !selectedUseCases.includes("All") || !selectedProducts.includes("All")

  // 필터링 로직
  const filteredQuickStarts = useMemo(() => {
    const term = searchTerm.toLowerCase()
    return data.filter((quickStart) => {
      // 추천 빠른 시작 항목은 탐색 섹션에서 제외
      if (featuredIds.includes(quickStart.id)) return false

      const matchesSearch = term === "" || quickStart.title.toLowerCase().includes(term) || (quickStart.description || "").toLowerCase().includes(term)

      // 선택 항목에 "All"이 포함된 경우 필터를 적용하지 않습니다. 그렇지 않으면
      // 빠른 시작의 모든 태그가 선택 항목에 포함될 때만 일치로 처리합니다. 예를 들어
      // "데이터 웨어하우징"을 선택하면 다른 사용 사례 태그(또는 "All" 태그)가
      // 함께 지정된 빠른 시작은 결과에서 제외됩니다.
      const useCases = quickStart.useCases || []
      const matchesUseCases = selectedUseCases.includes("All") || (useCases.length > 0 && useCases.every((uc) => selectedUseCases.includes(uc)))

      const products = quickStart.products || []
      const matchesProducts = selectedProducts.includes("All") || (products.length > 0 && products.every((p) => selectedProducts.includes(p)))

      return matchesSearch && matchesUseCases && matchesProducts
    })
  }, [data, searchTerm, selectedUseCases, selectedProducts, featuredIds])

  // Mintlify의 클라이언트 사이드 라우팅이 탐색기 스크롤 위치를 유지하는 대신,
  // 빠른 시작이 맨 위로 스크롤된 상태로 열리도록 전체 페이지 탐색을 강제합니다.
  // 수정자 키 또는 가운데 버튼 클릭은 기본 동작(새 탭에서 열기)으로 처리됩니다.
  const handleCardClick = (e, href) => {
    if (e.defaultPrevented) return
    if (e.metaKey || e.ctrlKey || e.shiftKey || e.altKey || e.button !== 0) return
    e.preventDefault()
    window.location.assign(withBase(href))
  }

  // `featured`에 나열된 순서대로 표시되는 추천 빠른 시작 항목입니다. 각 항목은
  // 배너 이미지를 유지하며, 데이터가 아직 로드되지 않은 경우에도 카드를 즉시
  // 클릭할 수 있도록 href가 기본 빠른 시작 경로로 폴백됩니다.
  const featuredQuickStarts = featured
    .map((f) => {
      const qs = data.find((q) => q.id === f.id)
      return {
        id: f.id,
        image: f.image,
        href: (qs && qs.href) || `/ko/get-started/quickstarts/${f.id}`,
        title: (qs && qs.title) || ""
      }
    })
    .filter((f) => f.image)

  // 항상 표시되는 필터 그룹 (접기 불가)
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
          {/* 추천 빠른 시작 section - full width banner image cards */}
          {featuredQuickStarts.length > 0 && (
            <div className="mb-12">
              <h2 className="text-2xl font-semibold text-gray-900 dark:text-zinc-50 mb-6">추천 빠른 시작</h2>
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
                  aria-label={showFilters ? "필터 숨기기" : "필터 표시"}
                  title={showFilters ? "필터 숨기기" : "필터 표시"}
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
                      <label className="block text-sm font-semibold text-gray-900 dark:text-zinc-50 mb-3">검색</label>
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
                          placeholder="빠른 시작 검색..."
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
                        <FilterGroup label="사용 사례" options={useCaseOptions} selectedOptions={selectedUseCases} onToggle={toggleUseCase} />
                        <FilterGroup label="제품 영역" options={productOptions} selectedOptions={selectedProducts} onToggle={toggleProduct} />
                      </div>
                    </div>

                    {/* Reset button */}
                    {hasActiveFilters && (
                      <button
                        onClick={resetFilters}
                        className="w-full text-sm font-medium px-4 py-2 rounded-lg transition-all cursor-pointer border border-gray-300 dark:border-white/20 hover:border-black dark:hover:border-[#FAFF69] bg-white dark:bg-[#1B1B18] text-black dark:text-white"
                      >
                        필터 초기화
                      </button>
                    )}
                  </div>
                </div>
              </div>
            </div>

            {/* Right content area */}
            <div className="flex-1 min-w-0">
              <h2 className="text-2xl font-semibold text-gray-900 dark:text-zinc-50 mb-6">빠른 시작 둘러보기</h2>

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
                          aria-label="이전 페이지"
                        >
                          <svg width="16" height="16" viewBox="0 0 16 16" fill="none" xmlns="http://www.w3.org/2000/svg">
                            <path d="M10 12L6 8L10 4" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round" />
                          </svg>
                        </button>
                        <span className="text-sm text-gray-600 dark:text-gray-400">
                          {currentPage} / {totalPages} 페이지
                        </span>
                        <button
                          onClick={() => hasNext && setCurrentPage((prev) => prev + 1)}
                          disabled={!hasNext}
                          className={`p-2 rounded-lg border transition-all ${
                            hasNext
                              ? "border-gray-300 dark:border-white/20 bg-white dark:bg-[#1B1B18] text-black dark:text-white hover:border-[#FAFF69] cursor-pointer"
                              : "border-gray-200 dark:border-white/10 bg-gray-50 dark:bg-[#1B1B18]/50 text-gray-300 dark:text-white/20 cursor-not-allowed"
                          }`}
                          aria-label="다음 페이지"
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
                  <p className="text-gray-600 dark:text-gray-400 text-lg block">검색 조건에 맞는 빠른 시작 가이드가 없습니다.</p>
                  <p className="text-gray-500 dark:text-gray-500 text-sm mt-2 block">필터 또는 검색어를 변경하여 다시 시도해 보십시오.</p>
                </div>
              )}
            </div>
          </div>
        </div>
      </div>
    </>
  )
}