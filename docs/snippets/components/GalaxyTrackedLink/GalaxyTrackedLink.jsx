/**
 * TrackedLink — anchor that fires a Galaxy analytics event on click.
 *
 * Mintlify equivalent of clickhouse-docs's `src/components/GalaxyTrackedLink`.
 * Renders a plain <a> and, when `window.galaxy` is available, calls
 * `window.galaxy.track(eventName, { interaction: "click" })` on click.
 * No-ops gracefully when galaxy isn't loaded (e.g. local preview, blocked
 * cookies). Pass-through props (target, rel, className, download, …) are
 * forwarded to the anchor.
 *
 * Usage:
 *   import { TrackedLink } from "/snippets/components/GalaxyTrackedLink/GalaxyTrackedLink.jsx";
 *
 *   <TrackedLink
 *     href="/examples/redis-logs-dashboard.json"
 *     download="redis-logs-dashboard.json"
 *     eventName="docs.redis_logs_monitoring.dashboard_download"
 *   >
 *     Download
 *   </TrackedLink>
 */
export const TrackedLink = ({ href, eventName, children, ...rest }) => {
  const handleClick = () => {
    try {
      if (typeof window !== "undefined" && window.galaxy && eventName) {
        window.galaxy.track(eventName, { interaction: "click" });
      }
    } catch (e) {
      // Tracking failures must not break navigation.
    }
  };
  return (
    <a href={href} onClick={handleClick} {...rest}>
      {children}
    </a>
  );
};