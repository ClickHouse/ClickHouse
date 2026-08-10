export const ReferenceExplorer = ({ index }) => {
  const readHashState = () => {
    if (typeof window === 'undefined') return { query: '', categories: ['All'] };
    const params = new URLSearchParams(window.location.hash.slice(1));
    const categories = params.getAll('category');
    return {
      query: params.get('q') || '',
      categories: categories.length ? categories : ['All'],
    };
  };

  const readShowFilters = () => {
    if (typeof window === 'undefined') return true;
    try {
      return localStorage.getItem('reference-show-filters') !== 'false';
    } catch {
      return true;
    }
  };

  const [searchTerm, setSearchTerm] = useState(() => readHashState().query);
  const [selectedCategories, setSelectedCategories] = useState(() => readHashState().categories);
  const [currentPage, setCurrentPage] = useState(1);
  const [activeIndex, setActiveIndex] = useState(-1);
  const [categoriesOpen, setCategoriesOpen] = useState(true);
  const [showFilters, setShowFilters] = useState(readShowFilters);
  const [isDesktop, setIsDesktop] = useState(true);
  const itemsPerPage = 10;

  const data = index || { categories: [], entries: [] };
  const assetBase = typeof window !== 'undefined' && window.location.pathname.startsWith('/docs')
    ? '/docs'
    : '';
  const withBase = path => path && path.startsWith('/') ? assetBase + path : path;

  const entries = useMemo(() => data.entries.map(entry => {
    const title = entry[0];
    const href = entry[1];
    const category = data.categories[entry[2]]?.name || 'Reference';
    const summary = entry[3] || '';
    const aliases = entry[4] || [];
    return {
      title,
      href,
      category,
      summary,
      isAnchor: href.includes('#'),
      searchNames: [title, ...aliases].map(value => value.toLowerCase()),
      searchSummary: summary.toLowerCase(),
    };
  }), [data.entries, data.categories]);

  const filteredEntries = useMemo(() => {
    const query = searchTerm.trim().toLowerCase();
    return entries
      .filter(entry => selectedCategories.includes('All') || selectedCategories.includes(entry.category))
      .map(entry => {
        if (!query) return { ...entry, rank: entry.isAnchor ? 1 : 0 };
        if (entry.searchNames.some(value => value === query)) return { ...entry, rank: 0 };
        if (entry.searchNames.some(value => value.startsWith(query))) return { ...entry, rank: 1 };
        if (entry.searchNames.some(value => value.includes(query))) return { ...entry, rank: 2 };
        if (entry.searchSummary.includes(query)) return { ...entry, rank: 3 };
        return null;
      })
      .filter(Boolean)
      .sort((a, b) => a.rank - b.rank || a.title.length - b.title.length || a.title.localeCompare(b.title));
  }, [entries, searchTerm, selectedCategories]);

  const totalPages = Math.max(1, Math.ceil(filteredEntries.length / itemsPerPage));
  const visibleEntries = filteredEntries.slice(
    (currentPage - 1) * itemsPerPage,
    currentPage * itemsPerPage,
  );
  const hasActiveFilters = searchTerm.length > 0 || !selectedCategories.includes('All');

  const resetPosition = () => {
    setCurrentPage(1);
    setActiveIndex(-1);
  };

  const updateSearch = event => {
    setSearchTerm(event.target.value);
    resetPosition();
  };

  const toggleCategory = category => {
    setSelectedCategories(previous => {
      if (category === 'All') return ['All'];
      const selected = previous.filter(value => value !== 'All');
      const next = selected.includes(category)
        ? selected.filter(value => value !== category)
        : [...selected, category];
      return next.length ? next : ['All'];
    });
    resetPosition();
  };

  const clearFilters = () => {
    setSearchTerm('');
    setSelectedCategories(['All']);
    resetPosition();
  };

  const changePage = page => {
    setCurrentPage(page);
    setActiveIndex(-1);
  };

  const handleSearchKeyDown = event => {
    if (!visibleEntries.length) return;
    if (event.key === 'ArrowDown') {
      event.preventDefault();
      setActiveIndex(value => Math.min(visibleEntries.length - 1, value + 1));
    } else if (event.key === 'ArrowUp') {
      event.preventDefault();
      setActiveIndex(value => Math.max(0, value - 1));
    } else if (event.key === 'Enter') {
      event.preventDefault();
      window.location.assign(withBase(visibleEntries[Math.max(0, activeIndex)].href));
    } else if (event.key === 'Escape') {
      setSearchTerm('');
      resetPosition();
    }
  };

  useEffect(() => {
    if (typeof window === 'undefined' || !window.matchMedia) return;
    const media = window.matchMedia('(min-width: 1024px)');
    const updateViewport = () => setIsDesktop(media.matches);
    updateViewport();
    media.addEventListener('change', updateViewport);
    return () => media.removeEventListener('change', updateViewport);
  }, []);

  useEffect(() => {
    if (typeof window === 'undefined') return;
    try {
      localStorage.setItem('reference-show-filters', String(showFilters));
    } catch {
      // The explorer remains usable when storage is unavailable.
    }
  }, [showFilters]);

  useEffect(() => {
    if (typeof window === 'undefined') return;
    const params = new URLSearchParams();
    if (searchTerm) params.set('q', searchTerm);
    selectedCategories
      .filter(category => category !== 'All')
      .forEach(category => params.append('category', category));
    const hash = params.toString();
    const nextUrl = `${window.location.pathname}${window.location.search}${hash ? `#${hash}` : ''}`;
    window.history.replaceState(null, '', nextUrl);
  }, [searchTerm, selectedCategories]);

  useEffect(() => {
    if (typeof window === 'undefined') return;
    const syncFromHash = () => {
      const state = readHashState();
      setSearchTerm(state.query);
      setSelectedCategories(state.categories);
      resetPosition();
    };
    window.addEventListener('hashchange', syncFromHash);
    return () => window.removeEventListener('hashchange', syncFromHash);
  }, []);

  return (
    <div
      className="relative my-8"
      style={{
        maxWidth: '1312px',
        marginLeft: 'max(0px, calc((100vw - 1312px) / 2 - 19rem))',
        marginRight: 'auto',
        paddingLeft: '1.75rem',
        paddingRight: '1.75rem',
      }}
    >
      <header className="max-w-3xl pt-5 mb-6">
        <h1 id="reference" className="m-0 text-4xl font-bold tracking-tight text-gray-900 dark:text-zinc-50">
          Reference
        </h1>
        <p className="mt-3 mb-0 text-base leading-7 text-gray-600 dark:text-gray-400">
          Find syntax and detailed documentation for functions, settings, engines, data types, formats, and system tables.
        </p>
      </header>

      <div className="mb-10">
        <label htmlFor="reference-search" className="block text-sm font-semibold text-gray-900 dark:text-zinc-50 mb-3">
          Search Reference
        </label>
        <div className="relative w-full">
          <svg
            aria-hidden="true"
            className="absolute pointer-events-none z-10"
            style={{ left: '16px', top: '50%', transform: 'translateY(-50%)', width: '18px', height: '18px', color: '#666' }}
            fill="none"
            stroke="currentColor"
            viewBox="0 0 24 24"
          >
            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth="2" d="M21 21l-6-6m2-5a7 7 0 1 1-14 0 7 7 0 0 1 14 0z" />
          </svg>
          <input
            id="reference-search"
            aria-label="Search Reference"
            type="search"
            value={searchTerm}
            onChange={updateSearch}
            onKeyDown={handleSearchKeyDown}
            aria-controls="reference-result-list"
            aria-activedescendant={visibleEntries[activeIndex] ? `reference-result-${activeIndex}` : undefined}
            placeholder="Search functions, settings, engines, data types, formats, and system tables..."
            className="w-full text-sm border rounded-xl focus:outline-none bg-white dark:bg-[#1B1B18] text-black dark:text-white border-gray-300 dark:border-gray-600 focus:border-black dark:focus:border-[#FAFF69]"
            style={{ height: '48px', padding: '0.5rem 1rem 0.5rem 3rem', lineHeight: '1.4', boxSizing: 'border-box' }}
          />
        </div>
      </div>

      <div className="flex flex-col lg:flex-row gap-6">
        <div
          className={isDesktop
            ? 'flex-shrink-0 transition-[width] duration-300 ease-in-out'
            : 'w-full'}
          style={isDesktop ? { width: showFilters ? '14rem' : '0px' } : undefined}
        >
          <aside
            aria-label="Reference filters"
            className="lg:sticky relative"
            style={isDesktop ? { top: '8.5rem' } : undefined}
          >
            <div
              aria-hidden="true"
              className="absolute bg-gray-200 dark:bg-white/10"
              style={isDesktop
                ? { left: '100%', top: 0, bottom: 0, width: '1px' }
                : { top: '100%', left: 0, right: 0, height: '1px' }}
            />
            <button
              type="button"
              onClick={() => setShowFilters(value => !value)}
              aria-label={showFilters ? 'Hide filters' : 'Show filters'}
              title={showFilters ? 'Hide filters' : 'Show filters'}
              className="flex items-center justify-center absolute z-20 cursor-pointer rounded-full border transition-colors border-gray-300 dark:border-white/20 hover:border-black dark:hover:border-[#FAFF69] bg-white dark:bg-[#1B1B18] text-gray-500 dark:text-gray-400 hover:text-black dark:hover:text-[#FAFF69] shadow-sm"
              style={isDesktop
                ? { left: '100%', top: '50%', width: '28px', height: '28px', transform: 'translate(-50%, -50%)' }
                : { top: '100%', left: '50%', width: '28px', height: '28px', transform: 'translate(-50%, -50%)' }}
            >
              <svg
                width="12"
                height="12"
                viewBox="0 0 12 12"
                fill="none"
                xmlns="http://www.w3.org/2000/svg"
                className="transition-transform duration-300"
                style={{ transform: `rotate(${isDesktop ? (showFilters ? 0 : 180) : (showFilters ? 90 : -90)}deg)` }}
              >
                <path d="M7.5 2.5L4 6L7.5 9.5" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round" />
              </svg>
            </button>

            <div
              className="overflow-hidden transition-all duration-300"
              style={isDesktop
                ? { width: '14rem', paddingRight: '1.25rem', opacity: showFilters ? 1 : 0, pointerEvents: showFilters ? 'auto' : 'none' }
                : { width: '100%', maxHeight: showFilters ? '1000px' : '0px', paddingBottom: showFilters ? '1.5rem' : '0px', opacity: showFilters ? 1 : 0, pointerEvents: showFilters ? 'auto' : 'none' }}
            >
              <div style={{ minWidth: '160px' }}>
                <button
                  type="button"
                  aria-expanded={categoriesOpen}
                  onClick={() => setCategoriesOpen(value => !value)}
                  className="text-sm font-medium transition-all cursor-pointer flex items-center justify-between w-full text-black dark:text-white"
                  style={{ padding: '4px 0', gap: '8px' }}
                >
                  <span className="font-semibold">
                    Category{selectedCategories.includes('All') ? '' : ` (${selectedCategories.length})`}
                  </span>
                  <svg
                    width="12"
                    height="12"
                    viewBox="0 0 12 12"
                    fill="none"
                    xmlns="http://www.w3.org/2000/svg"
                    className={`transition-transform duration-200 ${categoriesOpen ? 'rotate-180' : 'rotate-0'}`}
                  >
                    <path d="M2.5 4.5L6 8L9.5 4.5" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round" />
                  </svg>
                </button>

                {categoriesOpen && (
                  <div className="mt-1">
                    {[{ name: 'All', count: entries.length }, ...data.categories].map(category => {
                      const checked = selectedCategories.includes(category.name);
                      return (
                        <label
                          key={category.name}
                          className="flex items-center gap-2 py-1.5 cursor-pointer transition-colors"
                          onClick={event => { event.preventDefault(); toggleCategory(category.name); }}
                        >
                          <input type="checkbox" checked={checked} readOnly className="sr-only" />
                          <span
                            aria-hidden="true"
                            className="flex items-center justify-center w-4 h-4 rounded border flex-shrink-0"
                            style={{
                              borderColor: checked ? '#FAFF69' : 'rgba(156, 163, 175, 0.6)',
                              backgroundColor: checked ? '#FAFF69' : 'transparent',
                            }}
                          >
                            {checked && (
                              <svg width="10" height="10" viewBox="0 0 10 10" fill="none" xmlns="http://www.w3.org/2000/svg">
                                <path d="M2 5L4 7L8 3" stroke="black" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round" />
                              </svg>
                            )}
                          </span>
                          <span className="min-w-0 flex-1 truncate text-sm text-black dark:text-white">{category.name}</span>
                          <span className="text-xs tabular-nums text-gray-400">{category.count}</span>
                        </label>
                      );
                    })}
                  </div>
                )}

                {hasActiveFilters && (
                  <button
                    type="button"
                    onClick={clearFilters}
                    className="w-full mt-6 text-sm font-medium px-4 py-2 rounded-lg transition-all cursor-pointer border border-gray-300 dark:border-white/20 hover:border-black dark:hover:border-[#FAFF69] bg-white dark:bg-[#1B1B18] text-black dark:text-white"
                  >
                    Reset filters
                  </button>
                )}
              </div>
            </div>
          </aside>
        </div>

        <section aria-label="Reference results" className="flex-1 min-w-0">
          <p className="mt-0 mb-6 text-sm text-gray-500 dark:text-gray-400">
            {filteredEntries.length} {filteredEntries.length === 1 ? 'result' : 'results'}
          </p>

          {visibleEntries.length > 0 ? (
            <div id="reference-result-list" className="flex flex-col gap-2">
              {visibleEntries.map((entry, index) => (
                <a
                  id={`reference-result-${index}`}
                  key={entry.href}
                  href={withBase(entry.href)}
                  onMouseEnter={() => setActiveIndex(index)}
                  className={`group block rounded-lg border px-4 py-3 no-underline transition-all bg-white dark:bg-[#1B1B18] ${
                    index === activeIndex
                      ? 'border-black dark:border-[#FAFF69]'
                      : 'border-gray-200 dark:border-white/10 hover:border-black dark:hover:border-[#FAFF69]'
                  }`}
                >
                  <span className="block text-sm font-semibold text-gray-900 dark:text-zinc-50">{entry.title}</span>
                  {entry.summary && (
                    <span className="block text-sm text-gray-600 dark:text-gray-400 mt-0.5 line-clamp-2">
                      {entry.summary}
                    </span>
                  )}
                </a>
              ))}
            </div>
          ) : (
            <div className="text-center py-12 flex flex-col items-center">
              <p className="text-gray-600 dark:text-gray-400 text-lg block">No reference entries found.</p>
              <p className="text-gray-500 dark:text-gray-500 text-sm mt-2 block">Try adjusting your filters or search term.</p>
            </div>
          )}

          {totalPages > 1 && (
            <nav aria-label="Reference result pages" className="flex items-center justify-center gap-3 mt-8">
              <button
                type="button"
                disabled={currentPage === 1}
                onClick={() => changePage(Math.max(1, currentPage - 1))}
                aria-label="Previous page"
                className={`p-2 rounded-lg border transition-all ${currentPage > 1
                  ? 'border-gray-300 dark:border-white/20 bg-white dark:bg-[#1B1B18] text-black dark:text-white hover:border-[#FAFF69] cursor-pointer'
                  : 'border-gray-200 dark:border-white/10 bg-gray-50 dark:bg-[#1B1B18]/50 text-gray-300 dark:text-white/20 cursor-not-allowed'}`}
              >
                <svg width="16" height="16" viewBox="0 0 16 16" fill="none" xmlns="http://www.w3.org/2000/svg">
                  <path d="M10 12L6 8L10 4" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round" />
                </svg>
              </button>
              <span className="text-sm text-gray-600 dark:text-gray-400">Page {currentPage} / {totalPages}</span>
              <button
                type="button"
                disabled={currentPage === totalPages}
                onClick={() => changePage(Math.min(totalPages, currentPage + 1))}
                aria-label="Next page"
                className={`p-2 rounded-lg border transition-all ${currentPage < totalPages
                  ? 'border-gray-300 dark:border-white/20 bg-white dark:bg-[#1B1B18] text-black dark:text-white hover:border-[#FAFF69] cursor-pointer'
                  : 'border-gray-200 dark:border-white/10 bg-gray-50 dark:bg-[#1B1B18]/50 text-gray-300 dark:text-white/20 cursor-not-allowed'}`}
              >
                <svg width="16" height="16" viewBox="0 0 16 16" fill="none" xmlns="http://www.w3.org/2000/svg">
                  <path d="M6 4L10 8L6 12" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round" />
                </svg>
              </button>
            </nav>
          )}
        </section>
      </div>
    </div>
  );
};
