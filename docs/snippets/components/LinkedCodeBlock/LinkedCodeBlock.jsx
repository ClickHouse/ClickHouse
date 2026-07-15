export const LinkedCodeBlock = ({
  id,
  filename,
  language,
  startLine = 1,
  code: sourceCode,
  children,
}) => {
  const escapeRegExp = (value) => value.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')
  const codeValue = sourceCode ?? children
  const code = (typeof codeValue === 'string' ? codeValue : String(codeValue ?? '')).replace(/\n+$/, '')
  const lineCount = code.split('\n').length
  const [selectedLines, setSelectedLines] = useState([])
  const [selectionAnchor, setSelectionAnchor] = useState(null)
  const [copyStatus, setCopyStatus] = useState('')
  const [gutterMetrics, setGutterMetrics] = useState(null)
  const containerRef = useRef(null)

  const sourceLine = (lineIndex) => startLine + lineIndex - 1

  const makeHash = (firstLine, lastLine = firstLine) => {
    const first = sourceLine(firstLine)
    const last = sourceLine(lastLine)
    return last === first ? `#${id}-L${first}` : `#${id}-L${first}-L${last}`
  }

  const selectLines = async (lineIndex, extendSelection) => {
    const firstLine = extendSelection && selectionAnchor !== null ? selectionAnchor : lineIndex
    const rangeStart = Math.min(firstLine, lineIndex)
    const rangeEnd = Math.max(firstLine, lineIndex)
    const range = Array.from({ length: rangeEnd - rangeStart + 1 }, (_, index) => rangeStart + index)
    const hash = makeHash(rangeStart, rangeEnd)

    setSelectedLines(range)
    if (!extendSelection || selectionAnchor === null) {
      setSelectionAnchor(lineIndex)
    }

    if (typeof window === 'undefined') return

    const relativeUrl = `${window.location.pathname}${window.location.search}${hash}`
    window.history.replaceState(null, '', relativeUrl)

    const absoluteUrl = `${window.location.origin}${relativeUrl}`
    let copied = false

    try {
      await navigator.clipboard.writeText(absoluteUrl)
      copied = true
    } catch {
      const textArea = document.createElement('textarea')
      textArea.value = absoluteUrl
      textArea.style.position = 'fixed'
      textArea.style.opacity = '0'
      document.body.appendChild(textArea)
      textArea.select()
      copied = document.execCommand('copy')
      textArea.remove()
    }

    const selectedLabel = rangeStart === rangeEnd
      ? `line ${sourceLine(rangeStart)}`
      : `lines ${sourceLine(rangeStart)}–${sourceLine(rangeEnd)}`
    setCopyStatus(copied ? `Copied link to ${selectedLabel}` : 'Link added to the address bar')
  }

  useEffect(() => {
    if (typeof window === 'undefined') return

    const match = window.location.hash.match(
      new RegExp(`^#${escapeRegExp(id)}-L(\\d+)(?:-L(\\d+))?$`),
    )
    if (!match) return

    const firstLine = Number(match[1]) - startLine + 1
    const lastLine = Number(match[2] ?? match[1]) - startLine + 1
    if (firstLine < 1 || lastLine < firstLine || lastLine > lineCount) return

    setSelectionAnchor(firstLine)
    setSelectedLines(
      Array.from({ length: lastLine - firstLine + 1 }, (_, index) => firstLine + index),
    )

    const scrollTimer = window.setTimeout(() => {
      containerRef.current?.scrollIntoView({ block: 'center' })
    }, 0)

    return () => window.clearTimeout(scrollTimer)
  }, [id, lineCount, startLine])

  useEffect(() => {
    if (typeof window === 'undefined' || !containerRef.current) return

    const measureGutter = () => {
      const container = containerRef.current
      const pre = container?.querySelector('pre')
      const codeElement = pre?.querySelector('code') ?? pre
      if (!container || !pre || !codeElement) return

      const containerRect = container.getBoundingClientRect()
      const preRect = pre.getBoundingClientRect()
      const codeRect = codeElement.getBoundingClientRect()
      const codeBlockBackgroundRect = container.querySelector('.code-block-background')?.getBoundingClientRect()
      const filenameElement = Array.from(container.querySelectorAll('span')).find(
        (element) => element.childElementCount === 0 && element.textContent?.trim() === filename,
      )
      const filenameRect = filenameElement?.getBoundingClientRect()
      const firstHeaderButtonRect = container.querySelector('button')?.getBoundingClientRect()
      const computedLineHeight = Number.parseFloat(window.getComputedStyle(codeElement).lineHeight)
      const lineHeight = Number.isFinite(computedLineHeight)
        ? computedLineHeight
        : codeRect.height / lineCount

      setGutterMetrics({
        left: preRect.left - containerRect.left,
        top: codeRect.top - containerRect.top,
        width: (codeBlockBackgroundRect?.right ?? preRect.right) - preRect.left,
        lineHeight,
        statusLeft: filenameRect ? filenameRect.right - containerRect.left + 8 : 16,
        statusTop: filenameRect ? filenameRect.top - containerRect.top : 10,
        statusMaxWidth: filenameRect && firstHeaderButtonRect
          ? Math.max(firstHeaderButtonRect.left - filenameRect.right - 16, 0)
          : undefined,
      })
    }

    measureGutter()
    const resizeObserver = new ResizeObserver(measureGutter)
    resizeObserver.observe(containerRef.current)

    return () => resizeObserver.disconnect()
  }, [lineCount])

  useEffect(() => {
    if (!copyStatus || typeof window === 'undefined') return
    const statusTimer = window.setTimeout(() => setCopyStatus(''), 2200)
    return () => window.clearTimeout(statusTimer)
  }, [copyStatus])

  return (
    <div id={id} ref={containerRef} className="linked-code-block" style={{ position: 'relative' }}>
      <style>{`
        #${id} pre {
          padding-left: 0.25rem !important;
        }

        #${id} .code-block-background > div {
          padding-left: 0.25rem !important;
        }
      `}</style>

      <CodeBlock
        filename={filename}
        language={language}
        lines
      >{code}</CodeBlock>

      {gutterMetrics && selectedLines.map((lineIndex) => (
        <span
          key={lineIndex}
          aria-hidden="true"
          style={{
            position: 'absolute',
            zIndex: 1,
            left: gutterMetrics.left,
            top: gutterMetrics.top + ((lineIndex - 1) * gutterMetrics.lineHeight),
            width: gutterMetrics.width,
            height: gutterMetrics.lineHeight,
            background: 'rgba(56, 139, 253, 0.16)',
            pointerEvents: 'none',
          }}
        />
      ))}

      {gutterMetrics && (
        <div
          className="linked-code-block-gutter"
          aria-label={`Linkable line numbers for ${filename ?? id}`}
          style={{
            position: 'absolute',
            zIndex: 10,
            left: gutterMetrics.left,
            top: gutterMetrics.top,
            width: '1.75rem',
            userSelect: 'none',
          }}
        >
          {Array.from({ length: lineCount }, (_, index) => index + 1).map((lineIndex) => {
            const displayedLine = sourceLine(lineIndex)
            const selected = selectedLines.includes(lineIndex)
            return (
              <a
                key={lineIndex}
                href={makeHash(lineIndex)}
                aria-label={`Copy link to line ${displayedLine}`}
                aria-current={selected ? 'location' : undefined}
                onClick={(event) => {
                  event.preventDefault()
                  selectLines(lineIndex, event.shiftKey)
                }}
                style={{
                  display: 'flex',
                  height: `${gutterMetrics.lineHeight}px`,
                  cursor: 'pointer',
                }}
              />
            )
          })}
        </div>
      )}

      <span
        role="status"
        aria-live="polite"
        style={{
          position: 'absolute',
          left: gutterMetrics?.statusLeft ?? '1rem',
          top: gutterMetrics?.statusTop ?? '0.6rem',
          maxWidth: gutterMetrics?.statusMaxWidth,
          zIndex: 3,
          fontSize: '0.75rem',
          lineHeight: '1.5rem',
          overflow: 'hidden',
          textOverflow: 'ellipsis',
          whiteSpace: 'nowrap',
          opacity: copyStatus ? 0.8 : 0,
          transition: 'opacity 150ms ease',
          pointerEvents: 'none',
        }}
      >
        {copyStatus}
      </span>
    </div>
  )
}
