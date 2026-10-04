export const Glossary = ({ children, metadata = {} }) => {
  const nodeText = (node) => {
    if (node === null || node === undefined || typeof node === "boolean") return ""
    if (typeof node === "string" || typeof node === "number") return String(node)
    if (Array.isArray(node)) return node.map(nodeText).join(" ")
    return nodeText(node.props && node.props.children)
  }

  const entries = []
  const childNodes = Array.isArray(children) ? children : [children]
  let currentEntry

  childNodes.forEach((node) => {
    const id = node && node.props && node.props.id
    if (id) {
      currentEntry = {
        id,
        term: nodeText(node),
        content: [],
        ...(metadata[id] || {})
      }
      entries.push(currentEntry)
    } else if (currentEntry && node !== null && node !== undefined) {
      currentEntry.content.push(node)
    }
  })

  const [query, setQuery] = useState("")

  const searchForms = (value) => {
    const text = String(value)
    const lowercase = text.toLowerCase()
    const words = text
      .replace(/([a-z0-9])([A-Z])/g, "$1 $2")
      .toLowerCase()
      .replace(/[^a-z0-9]+/g, " ")
      .trim()
    return [...new Set([lowercase, words])]
  }

  const queryForms = searchForms(query.trim()).filter(Boolean)
  const matches = (value, exact = false) => searchForms(value).some((valueForm) => queryForms.some((queryForm) => (exact ? valueForm === queryForm : valueForm.includes(queryForm))))
  const matchRank = (entry) => {
    if (matches(entry.term, true)) return 2
    if ((entry.aliases || []).some((value) => matches(value, true))) return 1
    return 0
  }
  const visibleEntries =
    queryForms.length > 0
      ? entries.filter((entry) => [entry.term, ...(entry.aliases || []), nodeText(entry.content)].some((value) => matches(value))).sort((a, b) => matchRank(b) - matchRank(a))
      : entries

  return (
    <div className="not-prose glossary-browser">
      <div className="glossary-search-row">
        <label className="sr-only" htmlFor="glossary-search">
          Поиск по терминам и определениям глоссария
        </label>
        <div className="glossary-search-wrap">
          <svg aria-hidden="true" viewBox="0 0 20 20" className="glossary-search-icon">
            <path d="m17 17-3.7-3.7m1.7-4.8A6.5 6.5 0 1 1 2 8.5a6.5 6.5 0 0 1 13 0Z" fill="none" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" />
          </svg>
          <input id="glossary-search" type="search" value={query} onChange={(event) => setQuery(event.target.value)} placeholder="Поиск по терминам и определениям..." autoComplete="off" />
        </div>
        <span className="glossary-count" aria-live="polite">
          {visibleEntries.length} {visibleEntries.length === 1 ? "термин" : "терминов"}
        </span>
      </div>

      {visibleEntries.length > 0 ? (
        <div className="glossary-grid">
          {visibleEntries.map((entry) => (
            <article key={entry.id} className="glossary-entry">
              <h2 id={entry.id} className="glossary-entry-title">
                {entry.code ? <code>{entry.term}</code> : entry.term}
              </h2>
              {(entry.legacyIds || []).map((id) => (
                <span key={id} id={id} className="glossary-entry-legacy-anchor" aria-hidden="true" />
              ))}
              <div className="glossary-entry-description">{entry.content}</div>
              {entry.learnMore && (
                <a className="glossary-entry-link" href={entry.learnMore}>
                  Подробнее <span aria-hidden="true">→</span>
                </a>
              )}
            </article>
          ))}
        </div>
      ) : (
        <div className="glossary-empty">
          <p>По запросу «{query}» терминов не найдено.</p>
          <button type="button" onClick={() => setQuery("")}>
            Очистить поиск
          </button>
        </div>
      )}

      <style>{`
        .glossary-browser { margin-top: 1.5rem; }
        .glossary-search-row { display: flex; align-items: center; gap: .75rem; margin-bottom: 1.25rem; }
        .glossary-search-wrap { position: relative; flex: 1; }
        .glossary-search-icon { position: absolute; top: 50%; left: .85rem; width: 1rem; height: 1rem; color: #6b7280; transform: translateY(-50%); pointer-events: none; }
        .glossary-search-wrap input { width: 100%; height: 2.75rem; padding: 0 1rem 0 2.5rem; color: inherit; background: var(--background-light, #fff); border: 1px solid rgb(156 163 175 / .35); border-radius: .5rem; outline: none; }
        .glossary-search-wrap input:focus { border-color: #f1c40f; box-shadow: 0 0 0 3px rgb(253 255 117 / .35); }
        .dark .glossary-search-wrap input { background: var(--background-dark, #151515); border-color: rgb(107 114 128 / .45); }
        .glossary-count { flex: none; min-width: 4.5rem; color: #6b7280; font-size: .8rem; text-align: right; }
        .dark .glossary-count { color: #9ca3af; }
        .glossary-grid { display: grid; grid-template-columns: minmax(0, 1fr); gap: .85rem; }
        .glossary-entry { position: relative; padding: 1.15rem 1.25rem; background: var(--background-light, #fff); border: 1px solid rgb(156 163 175 / .3); border-radius: .65rem; }
        .dark .glossary-entry { background: var(--background-dark, #151515); border-color: rgb(107 114 128 / .35); }
        .glossary-entry-title { margin: 0 0 .55rem; scroll-margin-top: 6rem; font-size: 1.05rem; line-height: 1.35; }
        .glossary-entry-legacy-anchor { position: absolute; top: 0; scroll-margin-top: 6rem; }
        .glossary-entry-title code { font-size: .95em; }
        .glossary-entry-description { color: #4b5563; font-size: .9rem; line-height: 1.55; }
        .dark .glossary-entry-description { color: #d1d5db; }
        .glossary-entry-description p { margin: 0; }
        .glossary-entry-link { display: inline-block; margin-top: .75rem; color: inherit; font-size: .85rem; font-weight: 600; text-decoration: none; }
        .glossary-entry-link:hover { text-decoration: underline; }
        .glossary-empty { padding: 2.5rem 1rem; text-align: center; border: 1px dashed rgb(156 163 175 / .45); border-radius: .65rem; }
        .glossary-empty p { margin: 0 0 .75rem; color: #6b7280; }
        .glossary-empty button { padding: .45rem .75rem; color: inherit; background: transparent; border: 1px solid rgb(156 163 175 / .45); border-radius: .4rem; cursor: pointer; }
        @media (min-width: 768px) {
          .glossary-grid { grid-template-columns: repeat(2, minmax(0, 1fr)); }
        }
        @media (max-width: 520px) {
          .glossary-search-row { align-items: stretch; flex-direction: column; }
          .glossary-count { min-width: 0; text-align: left; }
        }
      `}</style>
    </div>
  )
}