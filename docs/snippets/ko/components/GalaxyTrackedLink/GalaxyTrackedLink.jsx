export const TrackedLink = ({ href, eventName, children, ...rest }) => {
  const handleClick = () => {
    try {
      if (typeof window !== "undefined" && window.galaxy && eventName) {
        window.galaxy.track(eventName, { interaction: "click" });
      }
    } catch (e) {
      // 추적 실패가 페이지 이동을 방해해서는 안 됩니다.
    }
  };
  return (
    <a href={href} onClick={handleClick} {...rest}>
      {children}
    </a>
  );
};