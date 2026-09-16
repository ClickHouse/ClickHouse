// Renders the "This guide integrates" indicator on ClickStack SDK guides.
//
// Shows one pill per SUPPORTED signal (logs / metrics / traces). Unsupported
// signals are simply omitted -- the section communicates what a guide can do,
// not what it cannot. Which signals a guide supports is passed inline via the
// `signals` prop as an array, e.g.
// <ClickStackIntegrates signals={['logs', 'traces']} />. A downstream service
// parses that prop from the MDX, so the prop is the single source of truth.
//
// `signals` accepts the canonical values 'logs', 'metrics', and 'traces'
// (case-insensitive). A comma-separated string is also accepted for resilience,
// but the array form is preferred for type safety.

export const ClickStackIntegrates = ({ signals = [] }) => {
  const list = Array.isArray(signals) ? signals : String(signals).split(",");
  const supported = list.map((s) => String(s).trim().toLowerCase());

  // ClickHouse brand yellow (#faff69) with dark text, matching
  // ClickHouseSupportedBadge; identical in light and dark mode.
  const pillStyle = {
    display: "inline-flex",
    alignItems: "center",
    padding: "0.125rem 0.625rem",
    borderRadius: "9999px",
    fontSize: "0.8125rem",
    fontWeight: 500,
    lineHeight: 1.6,
    color: "#161517",
    backgroundColor: "#faff69",
    border: "1px solid rgba(0, 0, 0, .1)",
    marginRight: "0.5rem",
  };

  return (
    <div style={{ margin: "0 0 1.25rem" }}>
      <span style={{ marginRight: "0.5rem" }}>This guide integrates:</span>
      {supported.includes("logs") && <span style={pillStyle}>Logs</span>}
      {supported.includes("metrics") && <span style={pillStyle}>Metrics</span>}
      {supported.includes("traces") && <span style={pillStyle}>Traces</span>}
    </div>
  );
};

export default ClickStackIntegrates;
