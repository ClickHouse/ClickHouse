export const galaxyOnClick = (eventName) => () => {
  try {
    if (typeof window !== "undefined" && window.galaxy && eventName) {
      window.galaxy.track(eventName, { interaction: "click" });
    }
  } catch (e) {
    // 추적 실패로 인해 페이지 이동이 중단되어서는 안 됩니다.
  }
};
export const galaxyTrack = (eventName, data) => {
  try {
    if (typeof window !== "undefined" && window.galaxy && eventName) {
      window.galaxy.track(eventName, data || {});
    }
  } catch (e) {}
};