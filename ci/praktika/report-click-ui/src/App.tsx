import { useState, useEffect, useRef } from 'react'
import { ClickUIProvider, Container, Text, Switch, Table, Link, Popover, Button, Flyout } from '@clickhouse/click-ui'
import './App.css'

interface TestResult {
  name: string
  status: string
  start_time: string | number
  duration: number
  info?: string
  links?: string[]
  results?: TestResult[]
}

interface PRResult {
  name: string
  status: string
  start_time: string | number
  duration: number
  results: TestResult[]
}

interface NestedTestResult extends TestResult {
  results?: NestedTestResult[]
}

function App() {
  const [theme, setTheme] = useState<'dark' | 'light'>('dark')
  const [data, setData] = useState<PRResult | NestedTestResult | null>(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)
  const [nameParams, setNameParams] = useState<string[]>([])
  const canvasRef = useRef<HTMLCanvasElement>(null)
  const [hoveredTask, setHoveredTask] = useState<{ name: string; x: number; y: number } | null>(null)

  const toggleTheme = () => {
    setTheme(theme === 'dark' ? 'light' : 'dark')
  }

  const normalizeName = (name: string): string => {
    // Replace spaces with underscores and remove special symbols
    return name
      .replace(/\s+/g, '_')
      .replace(/[^a-zA-Z0-9_-]/g, '')
      .toLowerCase()
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
    const shaParam = params.get('SHA')

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
    const shaParam = params.get('SHA')

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
        const shaParam = params.get('SHA')

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
          // Default URL for testing
          url = 'https://s3.amazonaws.com/clickhouse-test-reports/PRs/96792/dd4e76d16912546d3cdbcfb8c14b076b6ad28ee6/result_pr.json'
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
    return `${minutes}m ${remainingSeconds}s`
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
    // Extract the last part of the URL (filename)
    const parts = url.split('/')
    return parts[parts.length - 1] || url
  }

  const getColorForStatus = (status: string): string => {
    // Fixed colors based on status
    const colorMap: Record<string, string> = {
      success: '#22c55e',   // green
      failure: '#ef4444',   // red
      error: '#ef4444',     // red
      pending: '#eab308',   // yellow
      skipped: '#94a3b8',   // gray
      running: '#3b82f6',   // blue
    }
    return colorMap[status.toLowerCase()] || '#64748b' // default gray
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

  const createPopoverContent = (result: TestResult, navigateUrl?: string) => {
    return (
      <Container orientation='vertical' gap='sm' padding='md' style={{ maxWidth: '800px', maxHeight: '600px', overflow: 'auto' }}>
        {navigateUrl && (
          <Link href={navigateUrl} style={{ textDecoration: 'none' }}>
            <Button
              type="primary"
              label="Go to job results"
              style={{ width: '100%' }}
            />
          </Link>
        )}
        {result.info && (
          <Container orientation='vertical' gap='xs'>
            <Text style={{ fontWeight: 600 }}>Info:</Text>
            <Text style={{ whiteSpace: 'pre-wrap', fontFamily: 'monospace', fontSize: '12px' }}>
              {result.info}
            </Text>
          </Container>
        )}
        {result.links && result.links.length > 0 && (
          <Container orientation='vertical' gap='xs'>
            <Text style={{ fontWeight: 600 }}>Links:</Text>
            <Container orientation='vertical' gap='xs'>
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
            </Container>
          </Container>
        )}
        {result.results && result.results.length > 0 && (
          <Container orientation='vertical' gap='xs'>
            <Text style={{ fontWeight: 600 }}>Subresults ({result.results.length}):</Text>
            <Table
              headers={[
                { label: 'Status' },
                { label: 'Name' },
              ]}
              rows={result.results.map((subresult, subindex) => ({
                id: `sub-${subindex}`,
                items: [
                  { label: wrapWithPopover(subresult.status, subresult) },
                  { label: wrapWithPopover(subresult.name, subresult) },
                ],
              }))}
              size="sm"
            />
          </Container>
        )}
      </Container>
    )
  }

  const wrapWithPopover = (content: React.ReactNode, result: TestResult, navigateUrl?: string) => {
    const hasAdditionalInfo = result.info ||
                               (result.links && result.links.length > 0) ||
                               (result.results && result.results.length > 0) ||
                               navigateUrl

    if (!hasAdditionalInfo) {
      return <div style={{ width: '100%', height: '100%' }}>{content}</div>
    }

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
        <Popover.Content side="right" showArrow>
          {createPopoverContent(result, navigateUrl)}
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

  const rows = data?.results?.map((result, index) => {
    // Determine if navigation should be available
    // Only available if max_N > 0 OR status is success/failure/error
    const maxNameIndex = nameParams.length - 1
    const isClickableStatus = ['success', 'failure', 'error'].includes(result.status.toLowerCase())
    const shouldBeClickable = maxNameIndex > 0 || isClickableStatus

    const navigateUrl = shouldBeClickable ? buildUrlWithNewName(result.name) : undefined

    return {
      id: index,
      items: [
        { label: wrapWithPopover(result.status, result, navigateUrl), align: 'center' as const },
        { label: wrapWithPopover(result.name, result, navigateUrl), align: 'left' as const },
        { label: wrapWithPopover(formatDuration(result.duration), result, navigateUrl), align: 'center' as const },
        { label: wrapWithPopover(formatTime(result.start_time), result, navigateUrl), align: 'center' as const },
      ],
    }
  }) || []

  return (
    <ClickUIProvider theme={theme}>
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
            <Switch
              checked={theme === 'dark'}
              onCheckedChange={toggleTheme}
              label="Dark mode"
            />
          </div>
        </div>

        {/* Main Content */}
        <Container orientation='vertical' gap='none' padding='md' style={{ marginTop: '56px' }}>

          {loading && <Text>Loading test results...</Text>}

          {error && (
            <Text color='danger'>Error: {error}</Text>
          )}

          {data && !loading && (
            <Container orientation='vertical' gap='none'>
              <div style={{ padding: '12px 0', fontSize: '14px' }}>
                <Text>
                  status: <strong>{data.status}</strong> |
                  Start Time: <strong>{data.start_time ? (typeof data.start_time === 'number' ? new Date(data.start_time * 1000).toLocaleString() : new Date(data.start_time).toLocaleString()) : ''}</strong> |
                  Duration: <strong>{formatDuration(data.duration)}</strong>
                </Text>
              </div>

              <Table
                headers={headers}
                rows={rows}
                loading={loading}
              />
            </Container>
          )}
        </Container>
      </Container>
    </ClickUIProvider>
  )
}

export default App
