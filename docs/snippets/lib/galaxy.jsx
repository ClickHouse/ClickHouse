/**
 * Mintlify shim of clickhouse-docs's `src/lib/galaxy/galaxy`. Exports the
 * subset of helpers used by docs content; each gracefully no-ops when
 * `window.galaxy` isn't loaded (e.g. local preview).
 */
export const galaxyOnClick = (eventName) => () => {
  try {
    if (typeof window !== "undefined" && window.galaxy && eventName) {
      window.galaxy.track(eventName, { interaction: "click" });
    }
  } catch (e) {
    // Tracking failures must not break navigation.
  }
};

export const galaxyTrack = (eventName, data) => {
  try {
    if (typeof window !== "undefined" && window.galaxy && eventName) {
      window.galaxy.track(eventName, data || {});
    }
  } catch (e) {}
};
