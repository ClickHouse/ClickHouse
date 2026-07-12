export const galaxyOnClick = (eventName) => () => {
  try {
    if (typeof window !== "undefined" && window.galaxy && eventName) {
      window.galaxy.track(eventName, { interaction: "click" });
    }
  } catch (e) {
    // トラッキングの失敗でナビゲーションが中断されてはならない。
  }
};
export const galaxyTrack = (eventName, data) => {
  try {
    if (typeof window !== "undefined" && window.galaxy && eventName) {
      window.galaxy.track(eventName, data || {});
    }
  } catch (e) {}
};