export const AgentPrompt = ({ prompt, title = "Configuration assistée par agent", description, outline, outlineLabel = "Ce que l'agent va faire", repositoryUrl, repositoryLabel = "ClickHouse/agent-skills" }) => {
  const [copied, setCopied] = useState(false)

  const handleCopy = async () => {
    const copyWithTextArea = () => {
      const textArea = document.createElement("textarea")
      textArea.value = prompt
      textArea.style.position = "fixed"
      textArea.style.opacity = "0"
      document.body.appendChild(textArea)
      textArea.select()
      document.execCommand("copy")
      document.body.removeChild(textArea)
    }

    try {
      if (navigator?.clipboard?.writeText) {
        try {
          await navigator.clipboard.writeText(prompt)
        } catch {
          copyWithTextArea()
        }
      } else {
        copyWithTextArea()
      }
      setCopied(true)
      window.setTimeout(() => setCopied(false), 2000)
    } catch {
      // Copy failures should not affect the rest of the page.
    }
  }

  return (
    <div className="ch-agent-prompt-wrapper" data-mdast="ignore">
      <div className="ch-agent-prompt-main-row">
        <div className="ch-agent-prompt-left">
          <span className="ch-agent-prompt-title">{title}</span>
        </div>
        <div className="ch-agent-prompt-prompt-area">
          <code className="ch-agent-prompt-prompt-text">{prompt}</code>
        </div>
        <button type="button" className="ch-agent-prompt-copy-button" onClick={handleCopy} aria-label={copied ? "Copié" : "Copier le prompt"}>
          {copied ? (
            <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round" aria-hidden="true">
              <polyline points="20 6 9 17 4 12" />
            </svg>
          ) : (
            <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" aria-hidden="true">
              <rect x="9" y="9" width="13" height="13" rx="2" ry="2" />
              <path d="M5 15H4a2 2 0 0 1-2-2V4a2 2 0 0 1 2-2h9a2 2 0 0 1 2 2v1" />
            </svg>
          )}
          <span>{copied ? "Copié" : "Copier le prompt"}</span>
        </button>
      </div>
      {(description || repositoryUrl) && (
        <div className="ch-agent-prompt-sub-row">
          {description && <span className="ch-agent-prompt-description">{description}</span>}
          {repositoryUrl && (
            <a className="ch-agent-prompt-repository-link" href={repositoryUrl} target="_blank" rel="noopener noreferrer">
              {repositoryLabel}
            </a>
          )}
        </div>
      )}
      {outline?.length > 0 && (
        <details className="ch-agent-prompt-outline">
          <summary className="ch-agent-prompt-outline-summary">
            <svg width="12" height="12" viewBox="0 0 15 15" fill="none" xmlns="http://www.w3.org/2000/svg" className="ch-agent-prompt-outline-chevron" aria-hidden="true">
              <path
                d="M6.1584 3.13508C6.35985 2.94621 6.67627 2.95642 6.86514 3.15788L10.6151 7.15788C10.7954 7.3502 10.7954 7.64949 10.6151 7.84182L6.86514 11.8418C6.67627 12.0433 6.35985 12.0535 6.1584 11.8646C5.95694 11.6757 5.94673 11.3593 6.1356 11.1579L9.565 7.49985L6.1356 3.84182C5.94673 3.64036 5.95694 3.32394 6.1584 3.13508Z"
                fill="currentColor"
                fillRule="evenodd"
                clipRule="evenodd"
              />
            </svg>
            <span>{outlineLabel}</span>
          </summary>
          <ol className="ch-agent-prompt-outline-list">
            {outline.map((item, index) => (
              <li key={index}>{item}</li>
            ))}
          </ol>
        </details>
      )}
    </div>
  )
}