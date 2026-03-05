import { useState, useEffect, useRef } from 'react'
import { ClickUIProvider, Container, Text, Table, Link, Popover, Button, Badge, Icon, Tooltip, Panel, useToast, Dropdown, Logo } from '@clickhouse/click-ui'
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

interface NestedTestResult extends TestResult {
  results?: NestedTestResult[]
}

function AppContent({ theme, setTheme }: { theme: 'dark' | 'light', setTheme: (theme: 'dark' | 'light') => void }) {
  const [data, setData] = useState<PRResult | NestedTestResult | null>(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)
  const [nameParams, setNameParams] = useState<string[]>([])
  const canvasRef = useRef<HTMLCanvasElement>(null)
  const [sortByStatus, setSortByStatus] = useState<boolean>(true)
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

  // Update duration for running status or calculate from start_time if duration is 0
  useEffect(() => {
    if (!data) return

    const isRunning = data.status.toLowerCase() === 'running'
    const shouldCalculate = isRunning || data.duration === 0

    if (shouldCalculate && data.start_time) {
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

      // Update every second for running status
      if (isRunning) {
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

    // Check if any jobs are running
    const hasRunningJobs = data.results.some(r => r.status.toLowerCase() === 'running')

    if (hasRunningJobs) {
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

  const getStatusPriority = (status: string): number => {
    const statusLower = (status || '').toLowerCase()
    // Priority order: errors/failures first, then warnings, then success, then others
    if (statusLower.includes('error') || statusLower.includes('fail') || statusLower.includes('dropped')) return 0
    if (statusLower.includes('pending') || statusLower.includes('running')) return 1
    if (statusLower.includes('success') || statusLower === 'ok') return 2
    return 3 // other statuses
  }

  const normalizeName = (name: string): string => {
    return name
      .toLowerCase()
      .replace(/[^a-z0-9]/g, '_')
      .replace(/_+/g, '_')
      .replace(/_+$/, '')
  }

  const navigateToNestedResult = (
    rootData: PRResult | NestedTestResult,
    namePath: string[]
  ): PRResult | NestedTestResult => {
    // If no navigation path beyond name_1, return root
    if (namePath.length <= 2) {
      return rootData
    }

    // Navigate through results using name_2, name_3, etc.
    let current: PRResult | NestedTestResult = rootData

    for (let i = 2; i < namePath.length; i++) {
      const targetName = namePath[i]

      if (!current.results) {
        throw new Error(`No results found at level ${i}`)
      }

      const found = current.results.find((r) => r.name === targetName)

      if (!found) {
        throw new Error(`Result not found: ${targetName} at level ${i}`)
      }

      current = found as NestedTestResult
    }

    return current
  }

  const buildUrlWithNameRange = (maxIndex: number): string => {
    const params = new URLSearchParams(window.location.search)
    const newParams = new URLSearchParams()

    // Keep PR, REF, SHA parameters
    const prParam = params.get('PR')
    const refParam = params.get('REF')
    const shaParam = params.get('SHA') || params.get('sha')

    if (prParam) newParams.set('PR', prParam)
    if (refParam) newParams.set('REF', refParam)
    if (shaParam) newParams.set('SHA', shaParam)

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

    // Keep PR, REF, SHA parameters
    const prParam = params.get('PR')
    const refParam = params.get('REF')
    const shaParam = params.get('SHA') || params.get('sha')

    if (prParam) newParams.set('PR', prParam)
    if (refParam) newParams.set('REF', refParam)
    if (shaParam) newParams.set('SHA', shaParam)

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

        // Create cache key based on PR/REF and SHA to identify the session
        const cacheKey = `ci_ext_${prParam || refParam}_${shaParam}`

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
        let finalData: PRResult | NestedTestResult
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
    const isRunning = result.status.toLowerCase() === 'running'

    // If no start_time and no duration, return empty
    if (!result.start_time && (!result.duration || result.duration === 0)) {
      return ''
    }

    // For running jobs or jobs with duration 0, calculate from start_time
    if ((isRunning || result.duration === 0) && result.start_time) {
      const startTime = typeof result.start_time === 'number'
        ? result.start_time * 1000
        : new Date(result.start_time).getTime()
      const now = Date.now()
      const elapsed = (now - startTime) / 1000 // seconds
      return formatDuration(elapsed)
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
    return parts[parts.length - 1] || url
  }

  const getStatusBadge = (status: string) => {
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
      <span style={{ color: color, fontWeight: 'bold' }}>
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

  const getColorForStatus = (status: string): string => {
    const colorMap: Record<string, string> = {
      success: '#22c55e',
      failure: '#ef4444',
      error: '#ef4444',
      pending: '#eab308',
      skipped: '#94a3b8',
      running: '#3b82f6',
    }
    return colorMap[status.toLowerCase()] || '#64748b'
  }

  const drawTimeline = () => {
    const canvas = canvasRef.current
    if (!canvas || !data || !data.results || data.results.length === 0) {
      console.log('Timeline draw skipped:', { canvas: !!canvas, data: !!data, results: data?.results?.length })
      return
    }

    const ctx = canvas.getContext('2d')
    if (!ctx) return

    const results = data.results
    const lineWidth = 2
    const padding = 2
    const totalHeight = (lineWidth + padding) * results.length

    // Ensure minimum width
    const width = Math.max(canvas.clientWidth, 400)

    // Set canvas dimensions
    canvas.width = width
    canvas.height = totalHeight
    canvas.style.height = `${totalHeight}px`

    ctx.clearRect(0, 0, canvas.width, canvas.height)

    console.log('Drawing timeline:', { width: canvas.width, height: canvas.height, resultsCount: results.length })

    // Calculate time scale based on workflow duration
    // start_time is Unix timestamp (seconds since epoch) - use it directly
    const workflowStartTime = typeof data.start_time === 'string'
      ? new Date(data.start_time).getTime() / 1000
      : (data.start_time || 0)
    const workflowDuration = data.duration || 1
    const scaleX = canvas.width / workflowDuration

    console.log('Workflow:', { workflowStartTime, workflowDuration, canvasWidth: canvas.width, scaleX })

    results.forEach((task, index) => {
      const y = index * (lineWidth + padding)
      const height = lineWidth

      let x, width, color

      if (!task.start_time) {
        // Pending or skipped - full width bar
        x = 0
        width = canvas.width
        color = getColorForStatus(task.status)
      } else {
        // start_time is already a Unix timestamp (seconds) - use it directly
        const taskStartTime = typeof task.start_time === 'string'
          ? new Date(task.start_time).getTime() / 1000
          : task.start_time

        // Calculate how many seconds after workflow start this task began
        const relativeStartTime = taskStartTime - workflowStartTime
        x = relativeStartTime * scaleX

        if (!task.duration || task.duration === 0) {
          // Running or incomplete - extend to end
          width = canvas.width - x
          color = getColorForStatus('running')
        } else {
          // Completed - show actual duration
          width = task.duration * scaleX
          color = getColorForStatus(task.status)
        }

        // Debug first few tasks
        if (index < 3) {
          console.log(`Task ${index} (${task.name}):`, {
            taskStartTime,
            relativeStartTime,
            x,
            width,
            duration: task.duration
          })
        }
      }

      ctx.fillStyle = color
      ctx.fillRect(x, y, width, height)
    })
  }

  useEffect(() => {
    // Draw timeline whenever flyout might be visible
    const drawIfVisible = () => {
      if (canvasRef.current && data && nameParams.length <= 1) {
        drawTimeline()
      }
    }

    // Try drawing immediately and after a delay
    drawIfVisible()
    const timer = setTimeout(drawIfVisible, 200)

    return () => clearTimeout(timer)
  }, [data, nameParams, theme])

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
      } else {
        console.log('No toast function available')
      }
    }

    return (
      <Container orientation='vertical' gap='sm' padding='md' style={{ maxWidth: '800px', maxHeight: '600px', overflow: 'auto' }}>
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
              { label: 'Status' },
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
                  { label: wrapWithPopover(subNameWithLabels, subresult, undefined, namesPath) },
                ],
              }
            })}
            size="sm"
          />
        )}
        {result.links && result.links.length > 0 && (
          <Panel hasShadow padding='sm' fillWidth style={{ backgroundColor: 'rgba(0, 0, 0, 0.1)' }}>
            <div style={{
              display: 'grid',
              gridTemplateColumns: 'repeat(auto-fit, minmax(200px, 1fr))',
              gap: '8px',
              width: '100%'
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
        <div style={{ display: 'flex', justifyContent: 'flex-end', alignItems: 'center', gap: '12px', marginTop: '8px' }}>
          {navigateUrl && (
            <Link href={navigateUrl} style={{ textDecoration: 'none' }}>
              <Button
                type="primary"
                label="Open Result page"
              />
            </Link>
          )}
          {result.ext?.run_url && (
            <Link href={result.ext.run_url} target="_blank" rel="noopener noreferrer" style={{ display: 'flex', alignItems: 'center', padding: '4px' }}>
              <Logo name="github" theme={theme} size="sm" />
            </Link>
          )}
          <div
            onClick={copyUrlToClipboard}
            style={{
              cursor: 'pointer',
              padding: '4px',
              display: 'flex',
              alignItems: 'center',
              justifyContent: 'center'
            }}
          >
            <Icon name="copy" size="md" />
          </div>
        </div>
      </Container>
    )
  }

  const wrapWithPopover = (content: React.ReactNode, result: TestResult, navigateUrl?: string, namesPath: string[] = []) => {
    const hasAdditionalInfo = result.info ||
                               (result.links && result.links.length > 0) ||
                               (result.results && result.results.length > 0) ||
                               navigateUrl

    if (!hasAdditionalInfo) {
      return <div style={{ width: '100%', height: '100%' }}>{content}</div>
    }

    const currentPath = [...namesPath, result.name]

    return (
      <Popover>
        <Popover.Trigger asChild>
          <div style={{
            width: '100%',
            height: '100%',
            cursor: 'pointer',
            display: 'flex',
            alignItems: 'center',
            margin: '-8px -12px',
            padding: '8px 12px'
          }}>
            {content}
          </div>
        </Popover.Trigger>
        <Popover.Content side="right" showArrow style={{ zIndex: 1001 }}>
          {createPopoverContent(result, navigateUrl, currentPath, createToast)}
        </Popover.Content>
      </Popover>
    )
  }

  const headers = [
    { label: 'Status', width: '100px', align: 'center' as const },
    { label: 'Name', align: 'left' as const },
    { label: 'Duration', width: '120px', align: 'center' as const },
    { label: 'Start Time', width: '110px', align: 'center' as const },
  ]

  // Sort results by status if enabled
  const sortedResults = data?.results ? (sortByStatus
    ? [...data.results].sort((a, b) => getStatusPriority(a.status) - getStatusPriority(b.status))
    : data.results) : undefined

  const rows = sortedResults?.map((result, index) => {
    // Determine if navigation should be available
    // Only available if max_N > 0 OR status is success/failure/error
    const maxNameIndex = nameParams.length - 1
    const isClickableStatus = ['success', 'failure', 'error'].includes(result.status.toLowerCase())
    const shouldBeClickable = maxNameIndex > 0 || isClickableStatus

    const navigateUrl = shouldBeClickable ? buildUrlWithNewName(result.name) : undefined

    // Create name with labels
    const nameWithLabels = (
      <>
        {result.name}
        {renderLabels(result.ext?.labels, result.ext?.hlabels)}
      </>
    )

    return {
      id: index,
      items: [
        { label: wrapWithPopover(getStatusBadge(result.status), result, navigateUrl), align: 'center' as const },
        { label: wrapWithPopover(nameWithLabels, result, navigateUrl), align: 'left' as const },
        { label: wrapWithPopover(getJobDuration(result), result, navigateUrl), align: 'center' as const },
        { label: wrapWithPopover(formatTime(result.start_time), result, navigateUrl), align: 'center' as const },
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
            height: '56px',
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
              gap: '4px',
              overflow: 'auto'
            }}
          >
            {nameParams.length > 0 ? (
              nameParams.map((name, index) => (
                <div key={index} style={{ display: 'flex', alignItems: 'center', flexShrink: 0 }}>
                  <Text style={{ opacity: 0.6, margin: '0 4px' }}>/</Text>
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
            {/* TODO: Timeline feature - shows test execution timeline
            {nameParams.length <= 1 && (
              <Flyout>
                <Flyout.Trigger>
                  <Button
                    type="secondary"
                    label="i"
                    style={{ minWidth: '32px', width: '32px', padding: 0 }}
                    onClick={() => {
                      // Trigger redraw when flyout opens
                      setTimeout(() => drawTimeline(), 150)
                    }}
                  />
                </Flyout.Trigger>
                <Flyout.Content align="start" showOverlay strategy="fixed" width="500px">
                  <Flyout.Header title="Timeline" description="Test execution timeline" />
                  <Flyout.Body>
                    <Flyout.Element>
                      {data && data.results && data.results.length > 0 ? (
                        <div style={{ position: 'relative' }}>
                          <canvas
                            ref={canvasRef}
                            onMouseMove={(e) => {
                              const canvas = canvasRef.current
                              if (!canvas || !data.results) return

                              const rect = canvas.getBoundingClientRect()
                              const mouseY = e.clientY - rect.top

                              const lineWidth = 2
                              const padding = 2
                              const lineHeight = lineWidth + padding

                              const hoveredIndex = Math.floor(mouseY / lineHeight)

                              if (hoveredIndex >= 0 && hoveredIndex < data.results.length) {
                                const task = data.results[hoveredIndex]
                                setHoveredTask({
                                  name: task.name,
                                  x: e.clientX,
                                  y: e.clientY
                                })
                              } else {
                                setHoveredTask(null)
                              }
                            }}
                            onMouseLeave={() => setHoveredTask(null)}
                            style={{
                              width: '100%',
                              minHeight: '100px',
                              display: 'block',
                              border: '1px solid',
                              borderColor: theme === 'dark' ? 'rgba(255, 255, 255, 0.1)' : 'rgba(0, 0, 0, 0.1)',
                              cursor: 'pointer',
                            }}
                          />
                          {hoveredTask && (
                            <div
                              style={{
                                position: 'fixed',
                                left: `${hoveredTask.x + 10}px`,
                                top: `${hoveredTask.y - 10}px`,
                                backgroundColor: theme === 'dark' ? '#1D1D1D' : '#F9F9F9',
                                border: '1px solid',
                                borderColor: theme === 'dark' ? 'rgba(255, 255, 255, 0.2)' : 'rgba(0, 0, 0, 0.2)',
                                padding: '4px 8px',
                                borderRadius: '4px',
                                fontSize: '12px',
                                pointerEvents: 'none',
                                zIndex: 10000,
                                whiteSpace: 'nowrap',
                                boxShadow: '0 2px 8px rgba(0, 0, 0, 0.15)',
                              }}
                            >
                              {hoveredTask.name}
                            </div>
                          )}
                        </div>
                      ) : (
                        <Text>No timeline data available</Text>
                      )}
                    </Flyout.Element>
                  </Flyout.Body>
                </Flyout.Content>
              </Flyout>
            )}
            */}
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

        {/* Main Content - using plain div instead of Click UI Container to prevent overflow issues
            with Panel padding. TODO: improve responsive layout for narrow viewports */}
        <div style={{ marginTop: '56px', padding: '24px', width: '100%', boxSizing: 'border-box' }}>

          {loading && <Text>Loading test results...</Text>}

          {error && (
            <Text color='danger'>Error: {error}</Text>
          )}

          {data && !loading && (
            <div style={{ width: '100%', maxWidth: '100%', boxSizing: 'border-box' }}>
              <Panel hasBorder padding='md' orientation='vertical' gap='xs' alignItems='start' style={{ marginBottom: '16px', boxSizing: 'border-box' }}>
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
                    <Text>status:</Text>
                    {getStatusBadge(data.status)}
                    <Text>|</Text>
                    <Text>Start Time: <strong>{data.start_time ? (typeof data.start_time === 'number' ? new Date(data.start_time * 1000).toLocaleString() : new Date(data.start_time).toLocaleString()) : ''}</strong></Text>
                    <Text>|</Text>
                    <Text>Duration: <strong>{formatDuration(displayDuration)}</strong></Text>
                    {data.ext?.run_url && (
                      <>
                        <Text>|</Text>
                        <Link href={data.ext.run_url} target="_blank" rel="noopener noreferrer" style={{ display: 'flex', alignItems: 'center' }}>
                          <Logo name="github" theme={theme} size="sm" />
                        </Link>
                      </>
                    )}
                  </div>
                </Panel>

              {data.info && (
                <Panel hasShadow padding='md' orientation='vertical' gap='xs' alignItems='start' style={{ marginBottom: '16px', boxSizing: 'border-box' }}>
                  <Text style={{ whiteSpace: 'pre-wrap', fontSize: '13px' }}>{data.info}</Text>
                </Panel>
              )}

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

              <Table
                headers={headers}
                rows={rows}
                loading={loading}
                disableMobileListView={true}
              />
            </div>
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
                <Logo name="clickhouse" theme={theme} size="md" />
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
