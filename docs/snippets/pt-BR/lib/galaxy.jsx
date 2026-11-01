export const galaxyOnClick = (eventName) => () => {
  try {
    if (typeof window !== "undefined" && window.galaxy && eventName) {
      window.galaxy.track(eventName, { interaction: "click" });
    }
  } catch (e) {
    // Falhas no rastreamento não devem interromper a navegação.
  }
};
export const galaxyTrack = (eventName, data) => {
  try {
    if (typeof window !== "undefined" && window.galaxy && eventName) {
      window.galaxy.track(eventName, data || {});
    }
  } catch (e) {}
};