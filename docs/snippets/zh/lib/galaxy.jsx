export const galaxyOnClick = (eventName) => () => {
  try {
    if (typeof window !== "undefined" && window.galaxy && eventName) {
      window.galaxy.track(eventName, { interaction: "click" });
    }
  } catch (e) {
    // 跟踪失败不应影响页面跳转。
  }
};
export const galaxyTrack = (eventName, data) => {
  try {
    if (typeof window !== "undefined" && window.galaxy && eventName) {
      window.galaxy.track(eventName, data || {});
    }
  } catch (e) {}
};