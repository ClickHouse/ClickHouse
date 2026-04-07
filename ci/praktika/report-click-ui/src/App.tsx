import { useState, useEffect } from 'react'
import { ClickUIProvider, Container, Text, Table, Link, Dialog, Button, Badge, Icon, IconButton, Tooltip, Panel, useToast, Dropdown, Logo } from '@clickhouse/click-ui'
import './App.css'

interface TestResult {
  name: string
  status: string
  start_time: string | number
  duration: number
  info?: string
  links?: string[]
  results?: TestResult[]
  ext?: {
    labels?: string[]
    hlabels?: (string | [string, string] | { text?: string; label?: string; href?: string; url?: string })[]
    [key: string]: any
  }
}

interface PRResult {
  name: string
  status: string
  start_time: string | number
  duration: number
  results: TestResult[]
  info?: string
  links?: string[]
  ext?: {
    pr_title?: string
    git_branch?: string
    report_url?: string
    commit_sha?: string
    commit_message?: string
    repo_name?: string
    pr_number?: number
    run_url?: string
    change_url?: string
    workflow_name?: string
    [key: string]: any
  }
}

function AppContent({ theme, setTheme }: { theme: 'dark' | 'light', setTheme: (theme: 'dark' | 'light') => void }) {
  const [data, setData] = useState<PRResult | TestResult | null>(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)
  const [nameParams, setNameParams] = useState<string[]>([])
  const [sortByStatus, setSortByStatus] = useState<boolean>(true)
  const [mainSortColumn, setMainSortColumn] = useState<number | null>(null)
  const [mainSortDir, setMainSortDir] = useState<'asc' | 'desc'>('asc')
  const [displayDuration, setDisplayDuration] = useState<number>(0)
  const [, setTick] = useState(0)
  const { createToast } = useToast()
  const [topLevelExt, setTopLevelExt] = useState<any>(null)
  const [commits, setCommits] = useState<Array<{ sha: string; message: string }>>([])
  const [currentSha, setCurrentSha] = useState<string>('')

  // Generate dynamic favicon on mount
  useEffect(() => {
    const createFavicon = () => {
      const width = 64 // Higher resolution for sharper rendering
      const height = 64
      const lineColor = 'rgb(0, 0, 0)' // Black
      const lineWidth = 6
      const spaceWidth = 4
      const lineNumber = 5

      const canvas = document.createElement('canvas')
      canvas.width = width
      canvas.height = height
      const ctx = canvas.getContext('2d')

      if (!ctx) return

      ctx.clearRect(0, 0, width, height)

      let xStart = spaceWidth
      for (let i = 0; i < lineNumber; i++) {
        const yStart = Math.floor(Math.random() * height)

        ctx.fillStyle = lineColor
        for (let y = 0; y < height - spaceWidth; y++) {
          const yPos = (y + yStart) % height
          ctx.fillRect(xStart, yPos, lineWidth, 1)
        }

        xStart += lineWidth + spaceWidth
      }

      const faviconURL = canvas.toDataURL('image/png')
      let link = document.querySelector("link[rel='icon']") as HTMLLinkElement
      if (!link) {
        link = document.createElement('link')
        link.rel = 'icon'
        document.head.appendChild(link)
      }
      link.href = faviconURL
    }

    createFavicon()
  }, [])

  // Initialize sortByStatus from localStorage
  useEffect(() => {
    const savedSort = localStorage.getItem('sortByStatus')
    if (savedSort === null) {
      localStorage.setItem('sortByStatus', 'true')
      setSortByStatus(true)
    } else {
      setSortByStatus(savedSort === 'true')
    }
  }, [])

  // Set page title from PR title or commit message
  useEffect(() => {
    if (topLevelExt) {
      if (topLevelExt.pr_title) {
        document.title = topLevelExt.pr_title
      } else if (topLevelExt.commit_message) {
        document.title = topLevelExt.commit_message
      } else {
        document.title = 'praktika'
      }
    } else {
      document.title = 'praktika'
    }
  }, [topLevelExt])

  // Update duration for running status
  useEffect(() => {
    if (!data) return

    const statusLower = data.status.toLowerCase()
    const isInProgress = statusLower === 'running' || statusLower === 'pending'

    if (isInProgress && data.start_time) {
      const startTime = typeof data.start_time === 'number'
        ? data.start_time * 1000
        : new Date(data.start_time).getTime()

      const updateDuration = () => {
        const now = Date.now()
        const elapsed = (now - startTime) / 1000 // seconds
        setDisplayDuration(elapsed)
      }

      // Initial update
      updateDuration()

      // Update every second for in-progress statuses
      if (isInProgress) {
        const interval = setInterval(updateDuration, 1000)
        return () => clearInterval(interval)
      }
    } else {
      // Use the provided duration for finished statuses
      setDisplayDuration(data.duration)
    }
  }, [data])

  // Tick every second to update running job durations in table
  useEffect(() => {
    if (!data?.results) return

    // Check if any jobs are in progress
    const hasInProgressJobs = data.results.some(r => {
      const s = r.status.toLowerCase()
      return s === 'running' || s === 'pending'
    })

    if (hasInProgressJobs) {
      const interval = setInterval(() => {
        setTick(t => t + 1)
      }, 1000)
      return () => clearInterval(interval)
    }
  }, [data])

  const toggleTheme = () => {
    setTheme(theme === 'dark' ? 'light' : 'dark')
  }

  const toggleSortByStatus = () => {
    const newValue = !sortByStatus
    setSortByStatus(newValue)
    localStorage.setItem('sortByStatus', newValue ? 'true' : 'false')
  }

  const handleMainSort = (sortDir: 'asc' | 'desc', _header: any, index: number) => {
    setMainSortColumn(index)
    setMainSortDir(sortDir)
  }

  const getStatusPriority = (status: string): number => {
    const statusLower = (status || '').toLowerCase()
    if (statusLower.includes('error') || statusLower.includes('fail')) return 0
    if (statusLower.includes('dropped')) return 1
    if (statusLower.includes('pending') || statusLower.includes('running')) return 2
    if (statusLower.includes('success') || statusLower === 'ok') return 3
    return 4 // other statuses
  }

  const normalizeName = (name: string): string => {
    return name
      .toLowerCase()
      .replace(/[^a-z0-9]/g, '_')
      .replace(/_+/g, '_')
      .replace(/_+$/, '')
  }

  const navigateToNestedResult = (
    rootData: PRResult | TestResult,
    namePath: string[]
  ): PRResult | TestResult => {
    // If no navigation path beyond name_1, return root
    if (namePath.length <= 2) {
      return rootData
    }

    // Navigate through results using name_2, name_3, etc.
    let current: PRResult | TestResult = rootData

    for (let i = 2; i < namePath.length; i++) {
      const targetName = namePath[i]

      if (!current.results) {
        throw new Error(`No results found at level ${i}`)
      }

      const found = current.results.find((r) => r.name === targetName)

      if (!found) {
        throw new Error(`Result not found: ${targetName} at level ${i}`)
      }

      current = found as TestResult
    }

    return current
  }

  const buildUrlWithNameRange = (maxIndex: number): string => {
    const params = new URLSearchParams(window.location.search)
    const newParams = new URLSearchParams()

    // Keep PR, REF, SHA, url parameters
    const prParam = params.get('PR')
    const refParam = params.get('REF')
    const shaParam = params.get('SHA') || params.get('sha')
    const urlParam = params.get('url')

    if (prParam) newParams.set('PR', prParam)
    if (refParam) newParams.set('REF', refParam)
    if (shaParam) newParams.set('SHA', shaParam)
    if (urlParam) newParams.set('url', urlParam)

    // Keep name parameters from 0 to maxIndex
    for (let i = 0; i <= maxIndex; i++) {
      const value = params.get(`name_${i}`)
      if (value) {
        newParams.set(`name_${i}`, value)
      }
    }

    return `${window.location.pathname}?${newParams.toString()}`
  }

  const buildUrlWithNewName = (newName: string): string => {
    const params = new URLSearchParams(window.location.search)
    const newParams = new URLSearchParams()

    // Keep PR, REF, SHA, url parameters
    const prParam = params.get('PR')
    const refParam = params.get('REF')
    const shaParam = params.get('SHA') || params.get('sha')
    const urlParam = params.get('url')

    if (prParam) newParams.set('PR', prParam)
    if (refParam) newParams.set('REF', refParam)
    if (shaParam) newParams.set('SHA', shaParam)
    if (urlParam) newParams.set('url', urlParam)

    // Keep all existing name parameters
    let maxNameIndex = -1
    for (let i = 0; i < 100; i++) { // reasonable upper limit
      const value = params.get(`name_${i}`)
      if (value) {
        newParams.set(`name_${i}`, value)
        maxNameIndex = i
      } else {
        break
      }
    }

    // Add new name parameter at next index
    newParams.set(`name_${maxNameIndex + 1}`, newName)

    return `${window.location.pathname}?${newParams.toString()}`
  }

  useEffect(() => {
    const fetchData = async () => {
      try {
        setLoading(true)

        const params = new URLSearchParams(window.location.search)

        // Parse name_0, name_1, name_2, ... parameters
        const names: string[] = []
        let nameIndex = 0
        while (params.has(`name_${nameIndex}`)) {
          const value = params.get(`name_${nameIndex}`)
          if (value) {
            names.push(value)
          }
          nameIndex++
        }
        setNameParams(names)

        // Support multiple modes:
        // 1. Direct URL: ?url=https://...
        // 2. PR-based: ?PR=96792&SHA=dd4e76d&name_0=PR
        // 3. Branch-based: ?REF=master&SHA=dd4e76d&name_0=master

        let url: string
        const urlParam = params.get('url')
        const prParam = params.get('PR')
        const refParam = params.get('REF')
        let shaParam = params.get('SHA') || params.get('sha')

        // Fetch commits.json for SHA dropdown and resolve "latest"
        let commitsData: Array<{ sha: string; message: string }> = []
        if ((prParam || refParam) && shaParam) {
          try {
            const baseUrl = import.meta.env.DEV
              ? 'https://s3.amazonaws.com/clickhouse-test-reports'
              : `${window.location.origin}/clickhouse-test-reports`

            let pathType: string
            let refValue: string

            if (prParam) {
              pathType = 'PRs'
              refValue = prParam
            } else {
              pathType = 'REFs'
              refValue = refParam!
            }

            let commitsUrl = `${baseUrl}/${pathType}/${refValue}/commits.json`

            // In development, use proxy to avoid CORS issues
            if (import.meta.env.DEV && commitsUrl.startsWith('https://s3.amazonaws.com')) {
              commitsUrl = commitsUrl.replace('https://s3.amazonaws.com', '/s3-proxy')
            }

            const commitsResponse = await fetch(commitsUrl)
            if (commitsResponse.ok) {
              commitsData = await commitsResponse.json()
              setCommits(commitsData || [])

              // Resolve "latest" to actual SHA
              if (shaParam === 'latest' && commitsData.length > 0) {
                shaParam = commitsData[commitsData.length - 1].sha
              }
            }
          } catch (err) {
            console.error('Failed to fetch commits:', err)
          }
        }

        // Store current SHA (resolved from "latest" if needed)
        setCurrentSha(shaParam || '')

        // Create cache key based on PR/REF and SHA (or url) to identify the session
        const cacheKey = urlParam
          ? `ci_ext_url_${urlParam}`
          : `ci_ext_${prParam || refParam}_${shaParam}`

        if (urlParam) {
          // Mode 1: Direct URL provided
          url = urlParam
        } else if ((prParam || refParam) && shaParam) {
          // Mode 2 & 3: Construct URL from parameters
          const baseUrl = import.meta.env.DEV
            ? 'https://s3.amazonaws.com/clickhouse-test-reports'
            : `${window.location.origin}/clickhouse-test-reports`

          // Determine path type and reference value
          let pathType: string
          let refValue: string

          if (prParam) {
            pathType = 'PRs'
            refValue = prParam
          } else {
            pathType = 'REFs'
            refValue = refParam!
          }

          // Determine filename based on name parameters
          let filenameSuffix: string

          if (names.length === 0) {
            // No name parameters - use default
            filenameSuffix = prParam ? 'pr' : refParam?.toLowerCase() || 'master'
          } else if (names.length === 1) {
            // name_0 only - use it for filename
            filenameSuffix = names[0].toLowerCase()
          } else {
            // name_1 or more - use normalized name_1 for filename
            filenameSuffix = normalizeName(names[1])
          }

          const fileName = `result_${filenameSuffix}.json`
          url = `${baseUrl}/${pathType}/${refValue}/${shaParam}/${fileName}`
        } else {
          // No valid parameters provided
          throw new Error('Missing required parameters. Please provide: PR or REF with SHA, or url parameter')
        }

        // In development, use proxy to avoid CORS issues
        if (import.meta.env.DEV && url.startsWith('https://s3.amazonaws.com')) {
          url = url.replace('https://s3.amazonaws.com', '/s3-proxy')
        }

        const response = await fetch(url)
        if (!response.ok) {
          throw new Error(`HTTP error! status: ${response.status}`)
        }
        const jsonData = await response.json()

        // Try to load cached ext data first
        const cachedExtStr = sessionStorage.getItem(cacheKey)
        let cachedExt = null
        if (cachedExtStr) {
          try {
            cachedExt = JSON.parse(cachedExtStr)
          } catch (e) {
            console.error('Failed to parse cached ext:', e)
          }
        }

        // Store top-level ext for PR/commit info display
        if (jsonData.ext) {
          // If this ext has PR/commit info, cache it
          if (jsonData.ext.pr_number || jsonData.ext.commit_sha || jsonData.ext.pr_title || jsonData.ext.commit_message) {
            sessionStorage.setItem(cacheKey, JSON.stringify(jsonData.ext))
            setTopLevelExt(jsonData.ext)
          } else if (cachedExt) {
            // Use cached ext if current one doesn't have PR/commit info
            setTopLevelExt(cachedExt)
          } else {
            setTopLevelExt(jsonData.ext)
          }
        } else if (cachedExt) {
          // No ext in jsonData, but we have cached data
          setTopLevelExt(cachedExt)
        }

        // Navigate to nested result if name_2 or higher exists
        let finalData: PRResult | TestResult
        try {
          finalData = navigateToNestedResult(jsonData, names)
        } catch (navError) {
          throw new Error(
            navError instanceof Error
              ? `Navigation error: ${navError.message}`
              : 'Failed to navigate to nested result'
          )
        }

        setData(finalData)
        setError(null)
      } catch (err) {
        setError(err instanceof Error ? err.message : 'Failed to fetch data')
        console.error('Error fetching data:', err)
      } finally {
        setLoading(false)
      }
    }

    fetchData()
  }, [])

  const formatDuration = (seconds: number): string => {
    const minutes = Math.floor(seconds / 60)
    const remainingSeconds = Math.floor(seconds % 60)
    return `${minutes}m ${String(remainingSeconds).padStart(2, '0')}s`
  }

  const getJobDuration = (result: TestResult): string => {
    const statusLower = result.status.toLowerCase()
    const isInProgress = statusLower === 'running' || statusLower === 'pending'

    // If no start_time and no duration, return empty
    if (!result.start_time && (!result.duration || result.duration === 0)) {
      return ''
    }

    // For in-progress jobs, calculate elapsed time from start_time
    if (isInProgress && result.start_time) {
      const startTime = typeof result.start_time === 'number'
        ? result.start_time * 1000
        : new Date(result.start_time).getTime()
      const now = Date.now()
      const elapsed = (now - startTime) / 1000 // seconds
      return formatDuration(elapsed)
    }

    // For finished jobs with no duration, return empty
    if (!result.duration) {
      return ''
    }

    // Use provided duration for finished jobs
    return formatDuration(result.duration)
  }

  const formatTime = (timestamp: string | number): string => {
    if (!timestamp) return ''
    // If timestamp is a number, it's Unix timestamp in seconds
    const date = typeof timestamp === 'number'
      ? new Date(timestamp * 1000)
      : new Date(timestamp)
    return date.toLocaleTimeString()
  }

  const getLastPartOfUrl = (url: string): string => {
    const parts = url.split('/')
    const last = parts[parts.length - 1] || url
    return last.split('?')[0] || last
  }

  const getStatusBadge = (status: string, underline = false) => {
    const statusLower = status.toLowerCase()
    const statusUpper = status.toUpperCase()

    let color = '#64748b'

    // Map status to colors
    if (statusLower === 'success' || statusUpper === 'OK') {
      color = '#22c55e'
    } else if (statusLower === 'failure' || statusUpper === 'FAIL') {
      color = '#ef4444'
    } else if (statusLower === 'dropped') {
      color = '#a855f7'
    } else if (statusLower === 'error' || statusUpper === 'ERROR') {
      color = '#991b1b'
    } else if (statusLower === 'pending') {
      color = '#eab308'
    } else if (statusLower === 'running') {
      color = '#3b82f6'
    } else if (statusLower === 'skipped') {
      color = '#94a3b8'
    }

    return (
      <span style={{ color, fontWeight: 'bold', textDecoration: underline ? 'underline' : 'none' }}>
        {status}
      </span>
    )
  }

  const renderLabels = (labels?: string[], hlabels?: (string | [string, string] | { text?: string; label?: string; href?: string; url?: string })[]) => {
    if (!labels && !hlabels) return null

    return (
      <>
        {Array.isArray(labels) && labels.map((labelStr, index) => (
          <span key={`label-${index}`} style={{ marginLeft: '6px' }}>
            <Badge text={labelStr} state="neutral" size="sm" />
          </span>
        ))}
        {Array.isArray(hlabels) && hlabels.map((item, index) => {
          let text: string, href: string
          if (Array.isArray(item)) {
            [text, href] = item
          } else if (item && typeof item === 'object') {
            text = item.text || item.label || ''
            href = item.href || item.url || ''
          } else if (typeof item === 'string') {
            text = item
            href = ''
          } else {
            return null
          }

          if (!text) return null

          if (href) {
            return (
              <a
                key={`hlabel-${index}`}
                href={href}
                target="_blank"
                rel="noopener noreferrer"
                style={{ marginLeft: '6px', textDecoration: 'none' }}
              >
                <Badge text={text} state="info" size="sm" />
              </a>
            )
          } else {
            return (
              <span key={`hlabel-${index}`} style={{ marginLeft: '6px' }}>
                <Badge text={text} state="neutral" size="sm" />
              </span>
            )
          }
        })}
      </>
    )
  }

  const createPopoverContent = (result: TestResult, navigateUrl?: string, namesPath: string[] = [], showToast?: (options: any) => void) => {
    const copyUrlToClipboard = () => {
      const url = new URL(window.location.href)
      const params = new URLSearchParams(url.search)

      // Find the highest existing name_X index
      let maxIndex = -1
      params.forEach((_, key) => {
        const match = key.match(/^name_(\d+)$/)
        if (match) {
          maxIndex = Math.max(maxIndex, parseInt(match[1]))
        }
      })

      // Add new name parameters
      namesPath.forEach((name, idx) => {
        params.set(`name_${maxIndex + 1 + idx}`, name)
      })

      url.search = params.toString()
      navigator.clipboard.writeText(url.toString())

      if (showToast) {
        try {
          showToast({
            title: 'URL copied to clipboard',
            type: 'default'
          })
        } catch (e) {
          console.error('Toast error:', e)
        }
      }
    }

    return (
      <Container orientation='vertical' gap='sm' padding='none' style={{ maxHeight: '60vh', overflow: 'auto', paddingInline: '2px' }}>
        {result.info && (
          <Panel hasShadow padding='sm' orientation='vertical' gap='xs' alignItems='start' style={{ backgroundColor: 'rgba(0, 0, 0, 0.1)' }}>
            <Text style={{ whiteSpace: 'pre-wrap', fontFamily: 'monospace', fontSize: '12px' }}>
              {result.info}
            </Text>
          </Panel>
        )}
        {result.results && result.results.length > 0 && (
          <Table
            headers={[
              { label: 'Status', width: '100px' },
              { label: 'Name' },
            ]}
            rows={(sortByStatus
              ? [...result.results].sort((a, b) => getStatusPriority(a.status) - getStatusPriority(b.status))
              : result.results
            ).map((subresult, subindex) => {
              const subNameWithLabels = (
                <>
                  {subresult.name}
                  {renderLabels(subresult.ext?.labels, subresult.ext?.hlabels)}
                </>
              )
              return {
                id: `sub-${subindex}`,
                items: [
                  { label: wrapWithPopover(getStatusBadge(subresult.status), subresult, undefined, namesPath) },
                  { label: wrapWithPopover(subNameWithLabels, subresult, undefined, namesPath), style: { overflow: 'visible' } },
                ],
              }
            })}
            size="sm"
            style={{ tableLayout: 'auto' }}
          />
        )}
        {result.links && result.links.length > 0 && (
          <Panel hasShadow padding='sm' style={{ backgroundColor: 'rgba(0, 0, 0, 0.1)', width: 'fit-content' }}>
            <div style={{
              display: 'flex',
              flexDirection: 'column',
              gap: '8px',
            }}>
              {result.links.map((link, linkIndex) => (
                <Link
                  key={linkIndex}
                  href={link}
                  target="_blank"
                  rel="noopener noreferrer"
                  style={{ fontSize: '12px' }}
                >
                  {getLastPartOfUrl(link)}
                </Link>
              ))}
            </div>
          </Panel>
        )}
        <div style={{ display: 'flex', justifyContent: 'flex-end', alignItems: 'center', gap: '4px', marginTop: '8px' }}>
          {navigateUrl && (
            <Tooltip>
              <Tooltip.Trigger>
                <Link href={navigateUrl} style={{ textDecoration: 'none', display: 'flex' }}>
                  <IconButton icon="folder-open" type="ghost" />
                </Link>
              </Tooltip.Trigger>
              <Tooltip.Content showArrow>Open result page</Tooltip.Content>
            </Tooltip>
          )}
          {result.ext?.run_url && (
            <Tooltip>
              <Tooltip.Trigger>
                <Link href={result.ext.run_url} target="_blank" rel="noopener noreferrer" style={{ display: 'flex', alignItems: 'center', padding: '4px' }}>
                  <Logo name={'github'} size="sm" />
                </Link>
              </Tooltip.Trigger>
              <Tooltip.Content showArrow>Open GitHub Actions run</Tooltip.Content>
            </Tooltip>
          )}
          {!result.status.toLowerCase().includes('skip') && (
            <Tooltip>
              <Tooltip.Trigger>
                <IconButton icon="copy" type="ghost" onClick={copyUrlToClipboard} />
              </Tooltip.Trigger>
              <Tooltip.Content showArrow>Copy link to this result</Tooltip.Content>
            </Tooltip>
          )}
        </div>
      </Container>
    )
  }

  const wrapWithPopover = (content: React.ReactNode, result: TestResult, navigateUrl?: string, namesPath: string[] = [], justifyContent: string = 'flex-start') => {
    const hasAdditionalInfo = result.info ||
                               (result.links && result.links.length > 0) ||
                               (result.results && result.results.length > 0) ||
                               navigateUrl

    if (!hasAdditionalInfo) {
      return <div style={{ width: '100%', height: '100%', display: 'flex', alignItems: 'center', justifyContent }}>{content}</div>
    }

    const currentPath = [...namesPath, result.name]

    return (
      <Dialog>
        <Dialog.Trigger asChild>
          <div style={{
            width: '100%',
            height: '100%',
            cursor: 'pointer',
            display: 'flex',
            alignItems: 'center',
            justifyContent,
            margin: '-8px -12px',
            padding: '8px 12px'
          }}>
            {content}
          </div>
        </Dialog.Trigger>
        <Dialog.Content title={result.name} showClose>
          {createPopoverContent(result, navigateUrl, currentPath, createToast)}
        </Dialog.Content>
      </Dialog>
    )
  }

  const minimalCol = { style: { whiteSpace: 'nowrap' as const, width: '1px' } }
  const headers = [
    { label: 'Status',     ...minimalCol, isSortable: true, sortDir: mainSortColumn === 0 ? mainSortDir : undefined as any },
    { label: 'Name',                      isSortable: true, sortDir: mainSortColumn === 1 ? mainSortDir : undefined as any },
    { label: 'Duration',   ...minimalCol, isSortable: true, sortDir: mainSortColumn === 2 ? mainSortDir : undefined as any },
    { label: 'Start Time', ...minimalCol, isSortable: true, sortDir: mainSortColumn === 3 ? mainSortDir : undefined as any },
  ]

  const sortedResults = data?.results ? (() => {
    if (mainSortColumn !== null) {
      const dir = mainSortDir === 'asc' ? 1 : -1
      return [...data.results].sort((a, b) => {
        switch (mainSortColumn) {
          case 0: return dir * (getStatusPriority(a.status) - getStatusPriority(b.status))
          case 1: return dir * a.name.localeCompare(b.name)
          case 2: return dir * ((a.duration ?? 0) - (b.duration ?? 0))
          case 3: {
            const toEpoch = (t: string | number) => typeof t === 'number' ? t : new Date(t).getTime() / 1000
            return dir * ((toEpoch(a.start_time) || 0) - (toEpoch(b.start_time) || 0))
          }
          default: return 0
        }
      })
    }
    if (sortByStatus) {
      return [...data.results].sort((a, b) => getStatusPriority(a.status) - getStatusPriority(b.status))
    }
    return data.results
  })() : undefined

  const rows = sortedResults?.map((result, index) => {
    // Determine if navigation should be available
    // Only available if max_N > 0 OR status is success/failure/error
    const maxNameIndex = nameParams.length - 1
    const isTopLevel = maxNameIndex <= 0
    const isClickableStatus = ['success', 'failure', 'error'].includes(result.status.toLowerCase())
    const hasSubResults = result.results !== undefined && result.results.length > 0
    const shouldBeClickable = isTopLevel ? isClickableStatus : hasSubResults

    const navigateUrl = shouldBeClickable ? buildUrlWithNewName(result.name) : undefined

    // Create name with labels
    const nameWithLabels = (
      <>
        {result.name}
        {renderLabels(result.ext?.labels, result.ext?.hlabels)}
      </>
    )

    const nameCell = navigateUrl
      ? <Link href={navigateUrl} style={{ textDecoration: 'none' }}>{nameWithLabels}</Link>
      : nameWithLabels

    return {
      id: index,
      items: [
        { label: (() => {
          const hasDialog = !!(result.info || result.links?.length || result.results?.length || navigateUrl)
          return wrapWithPopover(getStatusBadge(result.status, hasDialog), result, navigateUrl, [], 'center')
        })(), style: { whiteSpace: 'nowrap' } },
        { label: nameCell },
        { label: getJobDuration(result), style: { whiteSpace: 'nowrap' } },
        { label: formatTime(result.start_time), style: { whiteSpace: 'nowrap' } },
      ],
    }
  }) || []

  return (
    <Container orientation='vertical' gap='none' style={{ minHeight: '100vh', alignItems: 'stretch' }}>
        {/* Header Bar */}
        <div
          style={{
            position: 'fixed',
            top: 0,
            left: 0,
            right: 0,
            backgroundColor: theme === 'dark' ? '#1a1a1a' : '#ffffff',
            borderBottom: '1px solid',
            borderColor: theme === 'dark' ? 'rgba(255, 255, 255, 0.1)' : 'rgba(0, 0, 0, 0.1)',
            height: '38px',
            display: 'flex',
            alignItems: 'center',
            justifyContent: 'space-between',
            padding: '0 16px',
            zIndex: 1000,
            gap: '16px'
          }}
        >
          {/* Navigation Container - Left Side */}
          <div
            style={{
              display: 'flex',
              alignItems: 'center',
              flex: 1,
              minWidth: 0,
              overflow: 'auto'
            }}
          >
            {nameParams.length > 0 ? (
              nameParams.map((name, index) => (
                <div key={index} style={{ display: 'flex', alignItems: 'center', flexShrink: 0 }}>
                  <span style={{ opacity: 0.6, paddingLeft: index === 0 ? 0 : '6px', paddingRight: '6px' }}>/</span>
                  <Link
                    href={buildUrlWithNameRange(index)}
                    style={{
                      textDecoration: 'none',
                      fontWeight: index === nameParams.length - 1 ? 600 : 400,
                      whiteSpace: 'nowrap'
                    }}
                  >
                    {name}
                  </Link>
                </div>
              ))
            ) : (
              <Text style={{ opacity: 0.6 }}>No navigation path</Text>
            )}
          </div>

          {/* Settings Container - Right Side */}
          <div
            style={{
              display: 'flex',
              alignItems: 'center',
              gap: '12px',
              flexShrink: 0
            }}
          >
            <Tooltip>
              <Tooltip.Trigger>
                <div
                  onClick={toggleSortByStatus}
                  style={{
                    cursor: 'pointer',
                    display: 'flex',
                    alignItems: 'center',
                    opacity: sortByStatus ? 1 : 0.5
                  }}
                >
                  <Icon name="sort-alt" size="md" />
                </div>
              </Tooltip.Trigger>
              <Tooltip.Content showArrow={true}>
                {sortByStatus ? 'Status sorting enabled' : 'Status sorting disabled'}
              </Tooltip.Content>
            </Tooltip>
            <Tooltip>
              <Tooltip.Trigger>
                <div
                  onClick={toggleTheme}
                  style={{ cursor: 'pointer', display: 'flex', alignItems: 'center' }}
                >
                  <Icon name="light-bulb-on" size="md" />
                </div>
              </Tooltip.Trigger>
              <Tooltip.Content showArrow={true}>
                Toggle theme
              </Tooltip.Content>
            </Tooltip>
          </div>
        </div>

        {/* Main Content */}
        <div className="main-content" style={{ marginTop: '38px' }}>

          {loading && <Text>Loading test results...</Text>}

          {error && (
            <Text color='danger'>Error: {error}</Text>
          )}

          {data && !loading && (
            <>
              <Panel hasBorder padding='md' orientation='vertical' gap='xs' alignItems='start' style={{ marginBottom: '16px' }}>
                {topLevelExt && (topLevelExt.pr_number > 0 || topLevelExt.commit_sha) && (
                  <div style={{ display: 'flex', alignItems: 'center', gap: '8px', fontSize: '16px', width: '100%', overflow: 'hidden' }}>
                    <div style={{ display: 'flex', alignItems: 'center', gap: '8px', flex: 1, minWidth: 0, overflow: 'hidden' }}>
                      <Icon name="git-merge" size="md" />
                      {topLevelExt.pr_number && topLevelExt.pr_number > 0 ? (
                        <>
                          <Link href={topLevelExt.change_url} target="_blank" rel="noopener noreferrer" style={{ fontWeight: 600 }}>
                            #{topLevelExt.pr_number}
                          </Link>
                          <Text>:</Text>
                          <Text style={{ overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>{topLevelExt.pr_title}</Text>
                        </>
                      ) : topLevelExt.commit_sha ? (
                        <>
                          <Link href={topLevelExt.change_url} target="_blank" rel="noopener noreferrer" style={{ fontWeight: 600, fontFamily: 'monospace' }}>
                            {topLevelExt.commit_sha.substring(0, 7)}
                          </Link>
                          <Text>:</Text>
                          <Text style={{ overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>{topLevelExt.commit_message}</Text>
                        </>
                      ) : null}
                    </div>
                    {commits.length > 0 && (
                      <div style={{ flexShrink: 0 }}>
                        <Dropdown>
                          <Dropdown.Trigger>
                            <Button type="secondary" label={currentSha ? currentSha.substring(0, 8) : 'SHA'} />
                          </Dropdown.Trigger>
                          <Dropdown.Content>
                            {commits.map((commit) => {
                              const shortSha = commit.sha.substring(0, 8)
                              const label = commit.message ? `${shortSha}: ${commit.message}` : shortSha
                              return (
                                <Dropdown.Item
                                  key={commit.sha}
                                  onClick={() => {
                                    const params = new URLSearchParams(window.location.search)
                                    params.set('SHA', commit.sha)
                                    window.location.search = params.toString()
                                  }}
                                >
                                  {label}
                                </Dropdown.Item>
                              )
                            })}
                            <Dropdown.Item
                              onClick={() => {
                                const params = new URLSearchParams(window.location.search)
                                const latestSha = commits[commits.length - 1]?.sha
                                if (latestSha) {
                                  params.set('SHA', latestSha)
                                  window.location.search = params.toString()
                                }
                              }}
                            >
                              latest
                            </Dropdown.Item>
                          </Dropdown.Content>
                        </Dropdown>
                      </div>
                    )}
                  </div>
                )}
                  <div style={{ fontSize: '14px', display: 'flex', alignItems: 'center', gap: '8px' }}>
                    <Text><strong>{data.name}</strong></Text>
                  </div>
                  <div style={{ fontSize: '14px', display: 'flex', alignItems: 'center', gap: '8px' }}>
                    <Text>Status:</Text>
                    {getStatusBadge(data.status)}
                    <Text>|</Text>
                    <Text>Start Time: <strong>{data.start_time ? (typeof data.start_time === 'number' ? new Date(data.start_time * 1000).toLocaleString() : new Date(data.start_time).toLocaleString()) : ''}</strong></Text>
                    <Text>|</Text>
                    <Text>Duration: <strong>{formatDuration(displayDuration)}</strong></Text>
                    {data.ext?.run_url && (
                      <>
                        <Text>|</Text>
                        <Link href={data.ext.run_url} target="_blank" rel="noopener noreferrer" style={{ display: 'flex', alignItems: 'center' }}>
                          <Logo name={'github'} size="sm" />
                        </Link>
                      </>
                    )}
                  </div>
                {data.info && (
                  <Text style={{ whiteSpace: 'pre-wrap', fontFamily: 'monospace', fontSize: '12px', padding: '8px', backgroundColor: 'rgba(0, 0, 0, 0.05)', borderRadius: '4px', boxSizing: 'border-box' }}>{data.info}</Text>
                )}
                </Panel>

              {data.links && data.links.length > 0 && (
                <Panel hasShadow padding='md' style={{ marginBottom: '16px', boxSizing: 'border-box' }}>
                  <div style={{
                    display: 'grid',
                    gridTemplateColumns: 'repeat(auto-fit, minmax(250px, 1fr))',
                    gap: '8px',
                    width: '100%'
                  }}>
                    {data.links.map((link, index) => (
                      <Link
                        key={index}
                        href={link}
                        target="_blank"
                        rel="noopener noreferrer"
                        style={{ fontSize: '13px' }}
                      >
                        {getLastPartOfUrl(link)}
                      </Link>
                    ))}
                  </div>
                </Panel>
              )}

              {rows.length > 0 && (
                <Table
                  headers={headers}
                  rows={rows}
                  loading={loading}
                  mobileLayout="scroll"
                  onSort={handleMainSort}
                  style={{ tableLayout: 'auto' }}
                />
              )}
            </>
          )}

          {/* Watermark footer - appears at bottom of page content */}
          <div style={{
            textAlign: 'center',
            padding: '24px',
            marginTop: '48px',
            borderTop: '1px solid',
            borderColor: theme === 'dark' ? 'rgba(255, 255, 255, 0.1)' : 'rgba(0, 0, 0, 0.1)',
            opacity: 0.5,
            fontSize: '13px'
          }}>
            <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'center', gap: '8px' }}>
              <Text>Made with</Text>
              <Link
                href="https://clickhouse.design/click-ui"
                target="_blank"
                rel="noopener noreferrer"
                style={{ textDecoration: 'none' }}
              >
                click-ui
              </Link>
              <Link
                href="https://clickhouse.com"
                target="_blank"
                rel="noopener noreferrer"
                style={{ display: 'flex', alignItems: 'center' }}
              >
                <Logo name={'clickhouse'} size="md" />
              </Link>
            </div>
          </div>
        </div>
      </Container>
  )
}

function App() {
  const [theme, setTheme] = useState<'dark' | 'light'>(() => {
    const savedTheme = localStorage.getItem('theme')
    return (savedTheme === 'dark' || savedTheme === 'light') ? savedTheme : 'dark'
  })

  const handleSetTheme = (newTheme: 'dark' | 'light') => {
    setTheme(newTheme)
    localStorage.setItem('theme', newTheme)
  }

  return (
    <ClickUIProvider theme={theme}>
      <AppContent theme={theme} setTheme={handleSetTheme} />
    </ClickUIProvider>
  )
}

export default App
