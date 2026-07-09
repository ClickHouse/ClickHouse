export const QuickStartsGrid = ({ quickStartsData = [], featured = [] }) => {
  const featuredIds = featured.map(f => f.id);
  const data = quickStartsData || [];
  const assetBase = (typeof window !== 'undefined' && window.location.pathname.startsWith('/docs')) ? '/docs' : '';
  const withBase = (p) => p && p.startsWith('/') ? assetBase + p : p;

  // Filter options. `value` is a stable slug matched against the tag slugs in
  // quickstarts-data.jsx (the generator emits the same slug form), so
  // filtering keeps working when the translation pipeline localizes the
  // labels. Only `label` is display text.
  const useCaseOptions = [
    { value: 'real-time-analytics', label: "실시간 분석" },
    { value: 'data-warehousing', label: "데이터 웨어하우징" },
    { value: 'observability', label: "관측성" },
    { value: 'ai-ml', label: "AI/ML" },
  ];
  const productOptions = [
    { value: 'self-managed', label: 'ClickHouse (오픈 소스)' },
    { value: 'cloud', label: 'ClickHouse Cloud' },
    { value: 'clickpipes', label: "ClickPipes" },
    { value: 'language-clients', label: "언어 클라이언트" },
    { value: 'clickstack', label: "ClickStack" },
    { value: 'chdb', label: "chDB" },
  ];

  // Only offer categories that at least one explorable quickstart belongs to
  // (an "all"-tagged quickstart belongs to every use case).
  const explorable = data.filter(qs => !featuredIds.includes(qs.id));
  const visibleUseCaseOptions = useCaseOptions.filter(o =>
    explorable.some(qs => { const u = qs.useCases || []; return u.includes('all') || u.includes(o.value); }));
  const visibleProductOptions = productOptions.filter(o =>
    explorable.some(qs => (qs.products || []).includes(o.value)));

  // All localStorage access goes through these guards. Storage may be absent
  // (SSR) or throw SecurityError (storage-restricted browsers or enterprise
  // policies); persistence is optional, so a failure means "not persisted"
  // rather than a render or effect exception that would take down the page.
  const readStored = (key) => {
    if (typeof window === 'undefined') return null;
    try {
      return localStorage.getItem(key);
    } catch {
      return null;
    }
  };
  const writeStored = (key, value) => {
    if (typeof window === 'undefined') return;
    try {
      localStorage.setItem(key, value);
    } catch {
      // Storage unavailable — persistence is best-effort, so drop it.
    }
  };

  // Read a persisted selection. A corrupted or hand-edited value must never
  // throw out of a useState initializer, which would crash the whole page
  // render — fall back to the default instead. Values not present in the
  // options (e.g. display strings persisted by an older version of this
  // component) are dropped. An empty selection means no filter.
  const readStoredSelection = (key, options) => {
    const raw = readStored(key);
    if (!raw) return [];
    try {
      const parsed = JSON.parse(raw);
      if (!Array.isArray(parsed)) return [];
      return parsed.filter(v => options.some(o => o.value === v));
    } catch {
      return [];
    }
  };

  // State management with localStorage
  const [searchTerm, setSearchTerm] = useState(() => readStored('quickstarts-search') || '');

  const [selectedUseCases, setSelectedUseCases] = useState(() => readStoredSelection('quickstarts-usecases', visibleUseCaseOptions));

  const [selectedProducts, setSelectedProducts] = useState(() => readStoredSelection('quickstarts-products', visibleProductOptions));

  const [currentPage, setCurrentPage] = useState(1);
  const itemsPerPage = 6;

  const [showFilters, setShowFilters] = useState(() => readStored('quickstarts-show-filters') !== 'false');

  // Track the lg breakpoint so the drawer can collapse left (desktop) or up
  // (mobile). Inline styles can't be responsive, so we branch on this in JS.
  const [isDesktop, setIsDesktop] = useState(true);
  useEffect(() => {
    if (typeof window === 'undefined' || !window.matchMedia) return;
    const mq = window.matchMedia('(min-width: 1024px)');
    const update = () => setIsDesktop(mq.matches);
    update();
    mq.addEventListener('change', update);
    return () => mq.removeEventListener('change', update);
  }, []);

  // Persist to localStorage
  useEffect(() => {
    writeStored('quickstarts-search', searchTerm);
  }, [searchTerm]);

  useEffect(() => {
    writeStored('quickstarts-usecases', JSON.stringify(selectedUseCases));
  }, [selectedUseCases]);

  useEffect(() => {
    writeStored('quickstarts-products', JSON.stringify(selectedProducts));
  }, [selectedProducts]);

  useEffect(() => {
    writeStored('quickstarts-show-filters', String(showFilters));
  }, [showFilters]);

  // Reset page when filters change
  useEffect(() => {
    setCurrentPage(1);
  }, [searchTerm, selectedUseCases, selectedProducts]);

  // Generic multi-select toggle; an empty selection means no filter.
  const makeToggle = (setter) => (value) => {
    setter(prev => prev.includes(value) ? prev.filter(v => v !== value) : [...prev, value]);
  };

  const toggleUseCase = makeToggle(setSelectedUseCases);
  const toggleProduct = makeToggle(setSelectedProducts);

  const resetFilters = () => {
    setSearchTerm('');
    setSelectedUseCases([]);
    setSelectedProducts([]);
  };

  const hasActiveFilters = searchTerm !== '' ||
    selectedUseCases.length > 0 ||
    selectedProducts.length > 0;

  // Filtering logic
  const filteredQuickStarts = useMemo(() => {
    const term = searchTerm.toLowerCase();
    return data.filter(quickStart => {
      // Exclude featured quickstarts from the explore section
      if (featuredIds.includes(quickStart.id)) return false;

      const matchesSearch = term === '' ||
        quickStart.title.toLowerCase().includes(term) ||
        (quickStart.description || '').toLowerCase().includes(term);

      // An empty selection means no filter. Otherwise a quickstart matches a
      // group if any of its tags is selected (groups combine with AND). A
      // quickstart tagged "all" applies to every use case, so it matches any
      // use-case selection.
      const useCases = quickStart.useCases || [];
      const matchesUseCases = selectedUseCases.length === 0 ||
        useCases.includes('all') ||
        useCases.some(uc => selectedUseCases.includes(uc));

      const products = quickStart.products || [];
      const matchesProducts = selectedProducts.length === 0 ||
        products.some(p => selectedProducts.includes(p));

      return matchesSearch && matchesUseCases && matchesProducts;
    });
  }, [data, searchTerm, selectedUseCases, selectedProducts, featuredIds]);

  // Force a full page navigation so the quickstart opens scrolled to the top,
  // instead of Mintlify's client-side routing keeping the explorer scroll
  // position. Modifier/middle clicks fall through to default (open in new tab).
  const handleCardClick = (e, href) => {
    if (e.defaultPrevented) return;
    if (e.metaKey || e.ctrlKey || e.shiftKey || e.altKey || e.button !== 0) return;
    e.preventDefault();
    window.location.assign(withBase(href));
  };

  // Featured quickstarts, in the order listed in `featured`. The banner art is
  // drawn in code from the title (see below), so no per-locale image is needed.
  // The href falls back to the conventional quickstart path if the data hasn't
  // loaded yet so the cards are clickable immediately.
  const featuredQuickStarts = featured
    .map(f => {
      const qs = data.find(q => q.id === f.id);
      return {
        id: f.id,
        href: (qs && qs.href) || `/ko/get-started/quickstarts/${f.id}`,
        title: (qs && qs.title) || '',
      };
    })
    .filter(f => f.title);

  // Always-visible filter group (not collapsible)
  const FilterGroup = ({ label, options, selectedOptions, onToggle }) => {
    const activeCount = selectedOptions.length;
    const displayLabel = activeCount > 0 ? `${label} (${activeCount})` : label;

    return (
      <div style={{ minWidth: '160px' }}>
        <div className="text-sm font-semibold text-black dark:text-white" style={{ padding: '4px 0' }}>
          {displayLabel}
        </div>
        <div className="mt-1">
          {options.map(option => (
            <label
              key={option.value}
              className="flex items-center gap-2 py-1.5 cursor-pointer transition-colors"
              onClick={(e) => { e.preventDefault(); onToggle(option.value); }}
            >
              <span
                className="flex items-center justify-center w-4 h-4 rounded border flex-shrink-0"
                style={{
                  borderColor: selectedOptions.includes(option.value) ? '#FAFF69' : 'rgba(156, 163, 175, 0.6)',
                  backgroundColor: selectedOptions.includes(option.value) ? '#FAFF69' : 'transparent',
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
    );
  };

  return (
    <>
      <div style={{ maxWidth: '1312px', marginLeft: 'max(0px, calc((100vw - 1312px) / 2 - 19rem))', marginRight: 'auto', paddingLeft: '1.75rem', paddingRight: '1.75rem' }}>
        <div className="my-8">
          {/* Featured quickstarts section - full width banner image cards */}
          {featuredQuickStarts.length > 0 && (
            <div className="mb-12">
              <h2 className="text-2xl font-semibold text-gray-900 dark:text-zinc-50 mb-6">추천 빠른 시작</h2>
              <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6">
                {featuredQuickStarts.map(quickStart => (
                  <a
                    key={quickStart.id}
                    href={quickStart.href}
                    onClick={(e) => handleCardClick(e, quickStart.href)}
                    className="group block rounded-xl overflow-hidden border border-gray-200 dark:border-white/10 transition-all hover:border-black dark:hover:border-[#FAFF69] hover:shadow-md"
                  >
                    {/* Banner art is drawn in code from the title so it
                        translates automatically — no per-locale PNG needed. */}
                    <div className="relative w-full aspect-[16/9] overflow-hidden bg-[#FAFF69] flex flex-col justify-center px-6 transition-transform duration-200 group-hover:scale-[1.02]">
                      {/* Decorative bar-chart motif, purely visual. */}
                      <div className="pointer-events-none absolute inset-y-0 right-6 flex items-center gap-2.5" aria-hidden="true">
                        <span className="w-3 rounded-sm bg-[#C4CB54]" style={{ height: '42%', transform: 'translateY(-12%)' }} />
                        <span className="w-3 rounded-sm bg-[#C4CB54]" style={{ height: '34%', transform: 'translateY(18%)' }} />
                        <span className="w-3 rounded-sm bg-[#C4CB54]" style={{ height: '64%', transform: 'translateY(-14%)' }} />
                        <span className="w-3 rounded-sm bg-[#C4CB54]" style={{ height: '46%', transform: 'translateY(20%)' }} />
                      </div>
                      <span className="relative z-10 pr-24 text-base md:text-lg font-bold leading-snug text-black line-clamp-4">
                        {quickStart.title}
                      </span>
                      {/* ClickHouse wordmark, inlined so it inherits currentColor. */}
                      <svg viewBox="0 0 161 34" fill="none" xmlns="http://www.w3.org/2000/svg" aria-hidden="true" className="absolute bottom-5 left-6 h-5 w-auto text-black">
                        <rect width="3.77758" height="33.9982" rx="0.918881" fill="currentColor" />
                        <rect x="7.55554" width="3.77758" height="33.9982" rx="0.918881" fill="currentColor" />
                        <rect x="15.1112" width="3.77758" height="33.9982" rx="0.918881" fill="currentColor" />
                        <rect x="22.6649" width="3.77758" height="33.9982" rx="0.918881" fill="currentColor" />
                        <rect x="30.2223" y="13.2227" width="3.77758" height="7.55515" rx="0.918881" fill="currentColor" />
                        <path d="M59.464 25.208C58.472 25.864 57.096 26.192 55.336 26.192C52.92 26.192 50.944 25.44 49.408 23.936C47.872 22.432 47.104 20.416 47.104 17.888C47.104 15.44 47.872 13.448 49.408 11.912C50.96 10.376 52.92 9.608 55.288 9.608C57.016 9.608 58.4 9.904 59.44 10.496V12.92C58.736 12.536 58.096 12.272 57.52 12.128C56.96 11.968 56.24 11.888 55.36 11.888C53.744 11.888 52.392 12.44 51.304 13.544C50.232 14.632 49.696 16.08 49.696 17.888C49.696 19.728 50.232 21.192 51.304 22.28C52.376 23.368 53.792 23.912 55.552 23.912C57.104 23.912 58.408 23.512 59.464 22.712V25.208ZM64.6593 26H62.2833V9.032H64.6593V26ZM70.5421 26H68.1661V13.832H70.5421V26ZM70.8301 10.112C70.8301 10.512 70.6861 10.856 70.3981 11.144C70.1101 11.432 69.7581 11.576 69.3421 11.576C68.9261 11.576 68.5741 11.44 68.2861 11.168C68.0141 10.88 67.8781 10.528 67.8781 10.112C67.8781 9.696 68.0141 9.344 68.2861 9.056C68.5741 8.768 68.9261 8.624 69.3421 8.624C69.7581 8.624 70.1101 8.768 70.3981 9.056C70.6861 9.344 70.8301 9.696 70.8301 10.112ZM82.5449 25.52C81.7609 26 80.7609 26.24 79.5449 26.24C77.6409 26.24 76.1209 25.64 74.9849 24.44C73.8489 23.24 73.2809 21.744 73.2809 19.952C73.2809 18.208 73.8489 16.72 74.9849 15.488C76.1369 14.24 77.6889 13.616 79.6409 13.616C80.6969 13.616 81.6649 13.84 82.5449 14.288V16.448C81.7609 15.952 80.8729 15.704 79.8809 15.704C78.6169 15.704 77.6009 16.104 76.8329 16.904C76.0809 17.704 75.7049 18.728 75.7049 19.976C75.7369 21.208 76.1129 22.224 76.8329 23.024C77.5529 23.808 78.5609 24.2 79.8569 24.2C80.8169 24.2 81.7129 23.936 82.5449 23.408V25.52ZM87.6018 26H85.2498V9.2H87.6018V26ZM90.4098 19.64L95.1858 26H92.3778L87.6978 19.64L92.2818 13.832H95.0418L90.4098 19.64ZM109.562 26H107.09V18.92H99.8178V26H97.3458V9.824H99.8178V16.784H107.09V9.824H109.562V26ZM122.384 15.44C123.392 16.608 123.896 18.104 123.896 19.928C123.896 21.752 123.376 23.256 122.336 24.44C121.312 25.624 119.896 26.216 118.088 26.216C116.296 26.216 114.888 25.632 113.864 24.464C112.84 23.296 112.328 21.816 112.328 20.024C112.328 18.2 112.856 16.688 113.912 15.488C114.968 14.272 116.36 13.664 118.088 13.664C119.944 13.664 121.376 14.256 122.384 15.44ZM114.776 19.952C114.776 21.152 115.072 22.168 115.664 23C116.256 23.816 117.064 24.224 118.088 24.224C119.176 24.224 120 23.824 120.56 23.024C121.136 22.224 121.424 21.208 121.424 19.976C121.424 18.712 121.152 17.68 120.608 16.88C120.064 16.064 119.216 15.656 118.064 15.656C117.024 15.656 116.216 16.072 115.64 16.904C115.064 17.72 114.776 18.736 114.776 19.952ZM136.459 26H134.107V23.912C133.467 25.4 132.203 26.144 130.315 26.144C129.115 26.144 128.163 25.792 127.459 25.088C126.755 24.368 126.403 23.384 126.403 22.136V13.832H128.779V21.56C128.779 22.392 128.979 23.032 129.379 23.48C129.795 23.912 130.363 24.128 131.083 24.128C131.995 24.128 132.723 23.808 133.267 23.168C133.827 22.528 134.107 21.624 134.107 20.456V13.832H136.459V26ZM147.373 22.64C147.373 23.728 146.989 24.584 146.221 25.208C145.453 25.816 144.413 26.12 143.101 26.12C141.661 26.12 140.461 25.88 139.501 25.4V23.168C140.621 23.856 141.781 24.2 142.981 24.2C143.605 24.2 144.085 24.072 144.421 23.816C144.773 23.56 144.949 23.224 144.949 22.808C144.949 22.456 144.813 22.144 144.541 21.872C144.269 21.584 143.989 21.376 143.701 21.248C143.429 21.104 142.957 20.888 142.285 20.6C140.381 19.816 139.429 18.664 139.429 17.144C139.429 16.072 139.829 15.232 140.629 14.624C141.429 14.016 142.429 13.712 143.629 13.712C144.941 13.712 145.965 13.888 146.701 14.24V16.376C145.917 15.912 144.909 15.68 143.677 15.68C143.117 15.68 142.669 15.808 142.333 16.064C142.013 16.32 141.853 16.656 141.853 17.072C141.853 17.184 141.861 17.296 141.877 17.408C141.909 17.504 141.957 17.608 142.021 17.72C142.085 17.816 142.141 17.904 142.189 17.984C142.253 18.048 142.349 18.12 142.477 18.2C142.605 18.28 142.701 18.352 142.765 18.416C142.845 18.464 142.965 18.528 143.125 18.608C143.301 18.688 143.429 18.752 143.509 18.8C143.589 18.832 143.725 18.896 143.917 18.992C144.125 19.072 144.269 19.128 144.349 19.16C145.293 19.576 146.029 20.048 146.557 20.576C147.101 21.088 147.373 21.776 147.373 22.64ZM158.892 25.352C157.996 25.864 156.812 26.12 155.34 26.12C153.452 26.12 151.948 25.552 150.828 24.416C149.708 23.28 149.148 21.792 149.148 19.952C149.148 18 149.66 16.472 150.684 15.368C151.708 14.248 153.012 13.688 154.596 13.688C156.148 13.688 157.38 14.176 158.292 15.152C159.204 16.112 159.66 17.504 159.66 19.328C159.66 19.824 159.612 20.344 159.516 20.888H151.524C151.684 21.944 152.132 22.752 152.868 23.312C153.604 23.872 154.532 24.152 155.652 24.152C156.948 24.152 158.028 23.848 158.892 23.24V25.352ZM154.644 15.632C153.844 15.632 153.156 15.928 152.58 16.52C152.004 17.096 151.644 17.936 151.5 19.04H157.38V18.608C157.348 17.696 157.1 16.976 156.636 16.448C156.172 15.904 155.508 15.632 154.644 15.632Z" fill="currentColor" />
                      </svg>
                    </div>
                  </a>
                ))}
              </div>
            </div>
          )}

          {/* Explore section with sidebar */}
          <div className="flex flex-col lg:flex-row gap-8">
            {/* Filter rail - collapses left on desktop, up on mobile; a toggle sits on the divider */}
            <div
              className={isDesktop
                ? "flex-shrink-0 transition-[width] duration-300 ease-in-out"
                : "w-full"}
              style={isDesktop ? { width: showFilters ? '16rem' : '0px' } : undefined}
            >
              <div className="lg:sticky relative" style={isDesktop ? { top: '8.5rem' } : undefined}>
                {/* Divider line: vertical on the panel's right (desktop), horizontal below it (mobile) */}
                <div
                  aria-hidden="true"
                  className="absolute bg-gray-200 dark:bg-white/10"
                  style={isDesktop
                    ? { left: '100%', top: 0, bottom: 0, width: '1px' }
                    : { top: '100%', left: 0, right: 0, height: '1px' }}
                />
                {/* Toggle button, centered on the divider line */}
                <button
                  onClick={() => setShowFilters(prev => !prev)}
                  aria-label={showFilters ? "필터 숨기기" : "필터 표시"}
                  title={showFilters ? "필터 숨기기" : "필터 표시"}
                  className="flex items-center justify-center absolute z-20 cursor-pointer rounded-full border transition-colors border-gray-300 dark:border-white/20 hover:border-black dark:hover:border-[#FAFF69] bg-white dark:bg-[#1B1B18] text-gray-500 dark:text-gray-400 hover:text-black dark:hover:text-[#FAFF69] shadow-sm"
                  style={isDesktop
                    ? { left: '100%', top: '50%', width: '28px', height: '28px', transform: 'translate(-50%, -50%)' }
                    : { top: '100%', left: '50%', width: '28px', height: '28px', transform: 'translate(-50%, -50%)' }}
                >
                  <svg
                    width="12" height="12" viewBox="0 0 12 12" fill="none" xmlns="http://www.w3.org/2000/svg"
                    className="transition-transform duration-300"
                    style={{ transform: `rotate(${isDesktop ? (showFilters ? 0 : 180) : (showFilters ? 90 : -90)}deg)` }}
                  >
                    <path d="M7.5 2.5L4 6L7.5 9.5" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round" />
                  </svg>
                </button>

                {/* Panel body: collapses by width (desktop) or max-height (mobile); overflow-hidden clips it.
                    The trailing padding keeps the controls off the divider line. */}
                <div
                  className="overflow-hidden transition-all duration-300"
                  style={isDesktop
                    ? { width: '16rem', paddingRight: '1.5rem', opacity: showFilters ? 1 : 0, pointerEvents: showFilters ? 'auto' : 'none' }
                    : { width: '100%', maxHeight: showFilters ? '1500px' : '0px', paddingBottom: showFilters ? '1.5rem' : '0px', opacity: showFilters ? 1 : 0, pointerEvents: showFilters ? 'auto' : 'none' }}
                >
                <div className="space-y-6">
                {/* Search input */}
                <div>
                  <label className="block text-sm font-semibold text-gray-900 dark:text-zinc-50 mb-3">
                    검색
                  </label>
                  <div className="relative w-full">
                    <svg
                      className="absolute pointer-events-none z-10"
                      style={{ left: '12px', top: '50%', transform: 'translateY(-50%)', width: '16px', height: '16px', color: '#666' }}
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
                      style={{ height: '42px', padding: '0.5rem 0.75rem 0.5rem 2.75rem', lineHeight: '1.4', boxSizing: 'border-box' }}
                    />
                  </div>
                </div>

                {/* Filters */}
                <div>
                  <div className="space-y-5">
                    <FilterGroup
                      label="사용 사례"
                      options={visibleUseCaseOptions}
                      selectedOptions={selectedUseCases}
                      onToggle={toggleUseCase}
                    />
                    <FilterGroup
                      label="제품 영역"
                      options={visibleProductOptions}
                      selectedOptions={selectedProducts}
                      onToggle={toggleProduct}
                    />
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
                    {filteredQuickStarts
                      .slice((currentPage - 1) * itemsPerPage, currentPage * itemsPerPage)
                      .map(quickStart => (
                        <a
                          key={quickStart.id}
                          href={quickStart.href}
                          onClick={(e) => handleCardClick(e, quickStart.href)}
                          className="group block rounded-lg border px-4 py-3 transition-all border-gray-200 dark:border-white/10 hover:border-black dark:hover:border-[#FAFF69] bg-white dark:bg-[#1B1B18]"
                        >
                          <div className="text-sm font-semibold text-gray-900 dark:text-zinc-50">
                            {quickStart.title}
                          </div>
                          <div className="text-sm text-gray-600 dark:text-gray-400 mt-0.5 line-clamp-2">
                            {quickStart.description}
                          </div>
                        </a>
                      ))}
                  </div>
                  {/* Pagination */}
                  {(() => {
                    const totalPages = Math.ceil(filteredQuickStarts.length / itemsPerPage);
                    if (totalPages <= 1) return null;
                    const hasPrev = currentPage > 1;
                    const hasNext = currentPage < totalPages;
                    return (
                      <div className="flex items-center justify-center gap-3 mt-8">
                        <button
                          onClick={() => hasPrev && setCurrentPage(prev => prev - 1)}
                          disabled={!hasPrev}
                          className={`p-2 rounded-lg border transition-all ${
                            hasPrev
                              ? 'border-gray-300 dark:border-white/20 bg-white dark:bg-[#1B1B18] text-black dark:text-white hover:border-[#FAFF69] cursor-pointer'
                              : 'border-gray-200 dark:border-white/10 bg-gray-50 dark:bg-[#1B1B18]/50 text-gray-300 dark:text-white/20 cursor-not-allowed'
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
                          onClick={() => hasNext && setCurrentPage(prev => prev + 1)}
                          disabled={!hasNext}
                          className={`p-2 rounded-lg border transition-all ${
                            hasNext
                              ? 'border-gray-300 dark:border-white/20 bg-white dark:bg-[#1B1B18] text-black dark:text-white hover:border-[#FAFF69] cursor-pointer'
                              : 'border-gray-200 dark:border-white/10 bg-gray-50 dark:bg-[#1B1B18]/50 text-gray-300 dark:text-white/20 cursor-not-allowed'
                          }`}
                          aria-label="다음 페이지"
                        >
                          <svg width="16" height="16" viewBox="0 0 16 16" fill="none" xmlns="http://www.w3.org/2000/svg">
                            <path d="M6 4L10 8L6 12" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round" />
                          </svg>
                        </button>
                      </div>
                    );
                  })()}
                </>
              ) : (
                <div className="text-center py-12 flex flex-col items-center">
                  <p className="text-gray-600 dark:text-gray-400 text-lg block">
                    검색 조건에 맞는 빠른 시작 가이드가 없습니다.
                  </p>
                  <p className="text-gray-500 dark:text-gray-500 text-sm mt-2 block">
                    필터 또는 검색어를 변경하여 다시 시도해 보십시오.
                  </p>
                </div>
              )}
            </div>
          </div>
        </div>
      </div>
    </>
  );
};