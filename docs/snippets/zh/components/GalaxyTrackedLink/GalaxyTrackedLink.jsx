export const TrackedLink = ({ href, eventName, children, ...rest }) => {
  const handleClick = () => {
    try {
      if (typeof window !== "undefined" && window.galaxy && eventName) {
        window.galaxy.track(eventName, { interaction: "click" });
      }
    } catch (e) {
      // 跟踪失败不应影响导航。
    }
  };
  return (
    <a href={href} onClick={handleClick} {...rest}>
      {children}
    </a>
  );
};